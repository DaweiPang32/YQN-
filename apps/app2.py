# ship_app_tab2.py —— 仅启用 按托盘发货（Tab2 的逻辑，去掉 Tab1）
# 功能：
# - 托盘重量/体积：重量只来自《托盘明细表》并按托盘求和；体积由长宽高（inch）计算为 CBM（每个托盘只计算一次，避免重复）
# - ETA/ATA（合并列）、ETD/ATD（Excel序列 45824 等）→ 日期字符串
# - 对客承诺送仓时间如“19-21”→ 与今天的天数差：x-y（锚定 ETA/ATA 的月份，缺失用当月）
# - 已发托盘读取自『发货追踪』，再次进入页面自动隐藏
# - 上传到『发货追踪』后，自动【部分更新】『运单全链路汇总』
#   仅更新以下列：客户单号、发出(ETD/ATD)、到港(ETA/ATA)、到BCF日期、到BCF卡车号、到BCF费用、发走日期、发走卡车号、发走费用
# - 只针对『发货追踪』里出现过的运单号进行汇总/更新
# - 兼容『bol自提明细』/『发货追踪』实际列名（卡车号/费用/日期/客户单号等）
# - 新增：在托盘展示中显示《托盘明细表》提交时写入的【托盘创建日期 / 托盘创建时间】
# - 防 429：退避重试 + 局部缓存 bust（不清全站）

import streamlit as st
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound, APIError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta, date
import calendar
import re
import random, time
import math

# ========= 授权范围 =========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def get_gspread_client():
    # 1) Cloud：优先从 st.secrets 读取（Streamlit Cloud 配置的机密）
    if "gcp_service_account" in st.secrets:
        sa_info = st.secrets["gcp_service_account"]  # 这是一个 dict（我们稍后在 Cloud 里配置）
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
        return gspread.authorize(creds)
    # 2) 本地：兼容你原来的 JSON 文件
    else:
        creds = Credentials.from_service_account_file("service_accounts.json", scopes=SCOPES)
        return gspread.authorize(creds)

client = get_gspread_client()

# ========= 表名配置 =========
SHEET_ARRIVALS_NAME   = "到仓数据表"       # ETD/ATD、ETA/ATA（合并）、对客承诺送仓时间、预计到仓时间（日）
SHEET_PALLET_DETAIL   = "托盘明细表"       # 托盘数据（重量/体积来自此表；体积由 L/W/H(inch) 计算为 CBM）
SHEET_SHIP_TRACKING   = "发货追踪test"         # 托盘维度出仓记录（分摊到托盘）
SHEET_BOL_DETAIL      = "bol自提明细"      # 到BCF 明细（分摊到运单）
SHEET_WB_SUMMARY      = "运单全链路汇总test"    # 仅部分更新

# ========= 通用工具 =========
def _norm_header(cols):
    return [c.replace("\u00A0"," ").replace("\n","").strip().replace(" ","") for c in cols]

def _to_num(x):
    try: return float(str(x).strip())
    except: return None

def _to_num_safe(x):
    try:
        s = str(x).strip().replace(",","")
        s = re.sub(r"[^\d\.\-]", "", s)
        return float(s)
    except:
        return None

def _is_blank(v):
    try:
        if v is None: return True
        if pd.isna(v): return True
        if isinstance(v, str) and v.strip()=="": return True
        return False
    except Exception:
        try: return bool(pd.isna(v))
        except Exception: return False

# ==== 退避重试的 get_all_values（遇 429 自动重试）====
def _safe_get_all_values(ws, value_render_option="UNFORMATTED_VALUE", date_time_render_option="SERIAL_NUMBER"):
    """对 get_all_values 做 429 退避重试，平滑瞬时读峰值。"""
    backoffs = [0.5, 1.0, 2.0, 4.0, 8.0]  # 最多 5 次，合计 ~15s
    for i, delay in enumerate([0.0] + backoffs):
        if delay:
            time.sleep(delay + random.random()*0.2)
        try:
            return ws.get_all_values(
                value_render_option=value_render_option,
                date_time_render_option=date_time_render_option
            )
        except APIError as e:
            msg = str(e)
            if ("Quota exceeded" in msg) or ("Read requests per minute" in msg) or ("429" in msg):
                if i < len(backoffs):
                    continue
            raise

# ==== 轻量 bust：只刷新相关缓存，不清全站 ====
def _bust(name: str):
    key = f"_bust_{name}"
    st.session_state[key] = int(st.session_state.get(key, 0)) + 1
    return st.session_state[key]

def _get_bust(name: str) -> int:
    return int(st.session_state.get(f"_bust_{name}", 0))

def _norm_waybill_str(v):
    if _is_blank(v): return ""
    s = str(v).strip()
    if s.endswith(".0"): s = s[:-2]
    try:
        f = float(s)
        if abs(f - round(f)) < 1e-9: s = str(int(round(f)))
    except: pass
    return s

# Excel/GS 序列起点
_BASE = datetime(1899, 12, 30)

def _coerce_excel_serial_sum(v):
    """
    将 v 合并为 Excel/GS 序列天数（可含小数）。
    兼容多格式，解析失败返回 None
    """
    try:
        if isinstance(v, (int, float)) and not pd.isna(v):
            return float(v)
    except Exception:
        pass
    if isinstance(v, str):
        s = v.strip()
        s = re.sub(r'[\u00A0\u2000-\u200B]', ' ', s)
        s = s.replace(',', '.')
        nums = re.findall(r'[-+]?\d+(?:\.\d+)?', s)
        total, ok = 0.0, False
        for n in nums:
            try:
                total += float(n); ok = True
            except Exception:
                pass
        if ok: return total
    if isinstance(v, (list, tuple)):
        total, ok = 0.0, False
        for x in v:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                continue
            try:
                xs = str(x).strip().replace(',', '.')
                total += float(xs); ok = True
            except Exception:
                pass
        if ok: return total
    return None

def _parse_sheet_value_to_date(v):
    """更安全的值->date 解析：优先看起来像日期的字符串，否则按序列数解析。"""
    if _is_blank(v): return None
    if isinstance(v, str):
        s = v.strip()
        if any(tok in s for tok in ["-", "/", "年", "月", "日", ":"]):
            dt = pd.to_datetime(s, errors="coerce")
            if pd.notna(dt): return dt.date()
    serial = _coerce_excel_serial_sum(v)
    if serial is not None:
        try:
            dt = _BASE + timedelta(days=float(serial))
            return dt.date()
        except Exception:
            pass
    try:
        dt = pd.to_datetime(v, errors="coerce")
        if pd.isna(dt): return None
        return dt.date()
    except Exception:
        return None

def _excel_serial_to_dt(v):
    """将任意形态的 Excel/GS 序列数或日期/时间字符串转为 datetime。"""
    if _is_blank(v): return None
    if isinstance(v, str):
        s = v.strip()
        if any(tok in s for tok in ["-", "/", "年", "月", "日", ":"]):
            ts = pd.to_datetime(s, errors="coerce")
            if pd.notna(ts): return ts.to_pydatetime()
    serial = _coerce_excel_serial_sum(v)
    if serial is not None:
        try:
            return _BASE + timedelta(days=float(serial))
        except Exception:
            pass
    try:
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts): return None
        return ts.to_pydatetime()
    except Exception:
        return None

def _fmt_date(d: date, out_fmt="%Y-%m-%d"):
    return d.strftime(out_fmt) if isinstance(d, date) else ""

def _parse_time_window_days(win: str):
    if not isinstance(win, str): return (None, None)
    s = win.strip()
    if "-" not in s: return (None, None)
    a, b = s.split("-", 1)
    try:
        sa, sb = int(a), int(b)
        if 1 <= sa <= 31 and 1 <= sb <= 31 and sa <= sb:
            return (sa, sb)
    except: pass
    return (None, None)

def _clamp_dom(year, month, dom):
    last = calendar.monthrange(year, month)[1]
    dom = max(1, min(last, dom))
    return date(year, month, dom)

def _promise_diff_days_str(win: str, anchor: date | None):
    sa, sb = _parse_time_window_days(win)
    if sa is None: return ""
    today = date.today()
    if anchor is None: anchor = today
    y, m = anchor.year, anchor.month
    start_d = _clamp_dom(y, m, sa)
    end_d   = _clamp_dom(y, m, sb)
    x = (start_d - today).days
    y2 = (end_d   - today).days
    return f"{x}-{y2}"

def _fmt_time_from_any(v, out_fmt="%H:%M"):
    dt = _excel_serial_to_dt(v)
    return dt.strftime(out_fmt) if isinstance(dt, datetime) else ""

def _split_dt_to_date_time_str(date_raw, time_raw):
    d_dt = _excel_serial_to_dt(date_raw)
    t_dt = _excel_serial_to_dt(time_raw)
    if isinstance(d_dt, datetime):
        date_str = d_dt.date().strftime("%Y-%m-%d")
    elif isinstance(t_dt, datetime):
        date_str = date.today().strftime("%Y-%m-%d")
    else:
        date_str = ""
    time_str = ""
    if isinstance(t_dt, datetime):
        time_str = t_dt.strftime("%H:%M")
    elif isinstance(d_dt, datetime):
        time_str = d_dt.strftime("%H:%M")
    return date_str, time_str

def _split_waybill_list(s):
    if _is_blank(s): return []
    parts = re.split(r"[,\，;\；、\|\/\s]+", str(s))
    return [_norm_waybill_str(p) for p in parts if _norm_waybill_str(p)]

def _first_nonblank_str(s):
    for x in s:
        if not _is_blank(x):
            return str(x).strip()
    return ""

# ========= 数据读取 =========
@st.cache_data(ttl=300)
def load_arrivals_df(_bust=0):
    ws = client.open(SHEET_ARRIVALS_NAME).sheet1
    data = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
    if not data: return pd.DataFrame()
    header = _norm_header(data[0])
    df = pd.DataFrame(data[1:], columns=header)

    for need in ["运单号","仓库代码","收费重"]:
        if need not in df.columns: df[need] = pd.NA

    vol_col = next((c for c in ["体积","CBM","体积m3","体积(m3)","体积（m3）"] if c in df.columns), None)
    if vol_col is None:
        df["体积"] = pd.NA
    else:
        df["体积"] = pd.to_numeric(df[vol_col], errors="coerce")

    etaata_col = None
    for cand in ["ETA/ATA","ETAATA"]:
        if cand in df.columns:
            etaata_col = cand; break
    if "ETD/ATD" not in df.columns: df["ETD/ATD"] = pd.NA
    if "对客承诺送仓时间" not in df.columns: df["对客承诺送仓时间"] = pd.NA

    eta_wh_col = None
    for cand in ["预计到仓时间（日）","预计到仓时间(日)","预计到仓时间日"]:
        if cand in df.columns:
            eta_wh_col = cand; break
    if eta_wh_col is None:
        df["预计到仓时间（日）"] = pd.NA
        eta_wh_col = "预计到仓时间（日）"

    df["运单号"] = df["运单号"].apply(_norm_waybill_str)
    df["仓库代码"] = df["仓库代码"].astype(str).str.strip()
    df["收费重"] = pd.to_numeric(df["收费重"], errors="coerce")

    if etaata_col is not None:
        df["_ETAATA_date"] = df[etaata_col].apply(_parse_sheet_value_to_date)
        df["ETA/ATA"] = df["_ETAATA_date"].apply(_fmt_date).replace("", pd.NA)
    else:
        df["_ETAATA_date"] = pd.NA
        df["ETA/ATA"] = pd.NA

    df["_ETD_ATD_date"] = df["ETD/ATD"].apply(_parse_sheet_value_to_date)
    df["ETD/ATD"] = df["_ETD_ATD_date"].apply(_fmt_date).replace("", pd.NA)

    df["_ETA_WH_date"] = df[eta_wh_col].apply(_parse_sheet_value_to_date)
    df["预计到仓时间（日）"] = df["_ETA_WH_date"].apply(_fmt_date).replace("", pd.NA)

    df = df.drop_duplicates(subset=["运单号"], keep="last")

    keep = ["仓库代码","运单号","收费重","体积",
            "ETA/ATA","ETD/ATD","对客承诺送仓时间","预计到仓时间（日）",
            "_ETAATA_date"]
    return df[keep]

@st.cache_data(ttl=300)
def load_pallet_detail_df(arrivals_df: pd.DataFrame | None = None, bol_cost_df: pd.DataFrame | None = None, _bust=0):
    """
    托盘维度：从《托盘明细表》聚合，并与《到仓数据表》匹配时间/承诺字段
    """
    ws = client.open(SHEET_PALLET_DETAIL).sheet1
    vals = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
    if not vals:
        return pd.DataFrame()

    header = _norm_header(vals[0])
    df = pd.DataFrame(vals[1:], columns=header)

    if "托盘号" not in df.columns:
        for cand in ["托盘ID","托盘编号","PalletID","PalletNo","palletid","palletno"]:
            if cand in df.columns:
                df = df.rename(columns={cand: "托盘号"})
                break
    if "托盘号" not in df.columns:
        df["托盘号"] = pd.NA

    if "仓库代码" not in df.columns:
        df["仓库代码"] = pd.NA

    if "运单号" not in df.columns:
        for cand in ["Waybill","waybill","运单编号"]:
            if cand in df.columns:
                df = df.rename(columns={cand: "运单号"})
                break
    if "运单号" not in df.columns:
        df["运单号"] = pd.NA

    df["托盘号"] = df["托盘号"].astype(str).str.strip()
    df["仓库代码"] = df["仓库代码"].astype(str).str.strip()
    df["运单号"] = df["运单号"].apply(_norm_waybill_str)

    weight_col = None
    for cand in ["托盘重量","托盘重","收费重","托盘收费重","计费重","计费重量","重量"]:
        if cand in df.columns:
            weight_col = cand
            break
    if weight_col is None:
        df["托盘重量"] = pd.NA
        weight_col = "托盘重量"
    df[weight_col] = pd.to_numeric(df[weight_col], errors="coerce")

    len_col = next((c for c in ["托盘长","长","长度","Length","length","L"] if c in df.columns), None)
    wid_col = next((c for c in ["托盘宽","宽","宽度","Width","width","W"] if c in df.columns), None)
    hei_col = next((c for c in ["托盘高","高","高度","Height","height","H"] if c in df.columns), None)

    qty_col = next((c for c in [
        "箱数","箱","件数","箱件数","Packages","Package","Cartons","Carton",
        "Qty","QTY","数量"
    ] if c in df.columns), None)
    if qty_col is None:
        df["箱数"] = pd.NA
        qty_col = "箱数"
    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce")

    INCH_TO_M = 0.0254
    def _cbm_row(r):
        if not all([len_col, wid_col, hei_col]):
            return None
        try:
            L = float(pd.to_numeric(r.get(len_col), errors="coerce"))
            W = float(pd.to_numeric(r.get(wid_col), errors="coerce"))
            H = float(pd.to_numeric(r.get(hei_col), errors="coerce"))
            if L > 0 and W > 0 and H > 0:
                return (L * W * H) * (INCH_TO_M ** 3)
        except Exception:
            pass
        return None
    df["_cbm_row"] = df.apply(_cbm_row, axis=1)

    def _first_valid_num(s):
        s_num = pd.to_numeric(s, errors="coerce").dropna()
        return float(s_num.iloc[0]) if len(s_num) > 0 else None

    def _wb_list(s):
        vals = [x for x in s if isinstance(x, str) and x.strip()]
        return vals

    create_date_col = next((c for c in ["托盘创建日期","创建日期","PalletCreateDate","CreateDate"] if c in df.columns), None)
    create_time_col = next((c for c in ["托盘创建时间","创建时间","PalletCreateTime","CreateTime"] if c in df.columns), None)
    if create_date_col is None:
        df["托盘创建日期"] = ""
        create_date_col = "托盘创建日期"
    if create_time_col is None:
        df["托盘创建时间"] = ""
        create_time_col = "托盘创建时间"

    agg_dict = {
        "托盘重量": (weight_col, _first_valid_num),
        "托盘体积": ("_cbm_row", _first_valid_num),
        "运单清单_list": ("运单号", _wb_list),
        "托盘创建日期_raw": (create_date_col, _first_nonblank_str),
        "托盘创建时间_raw": (create_time_col, _first_nonblank_str),
    }
    if len_col: agg_dict["托盘长in"] = (len_col, _first_valid_num)
    if wid_col: agg_dict["托盘宽in"] = (wid_col, _first_valid_num)
    if hei_col: agg_dict["托盘高in"] = (hei_col, _first_valid_num)

    base = (
        df.groupby(["托盘号", "仓库代码"], as_index=False, dropna=False)
          .agg(**agg_dict)
    )

    # 依赖表：尽量复用传入的数据，减少重复读
    arrivals = arrivals_df if arrivals_df is not None else load_arrivals_df(_bust=_get_bust("arrivals"))

    df_join = df.merge(
        arrivals[["运单号", "ETA/ATA", "ETD/ATD", "对客承诺送仓时间", "_ETAATA_date"]],
        on="运单号", how="left"
    )

    # 客户单号映射（优先来自『bol自提明细』）
    bol_cust_df = bol_cost_df if bol_cost_df is not None else load_bol_waybill_costs(_bust=_get_bust("bol_detail"))
    cust_map = {}
    if not bol_cust_df.empty and "运单号" in bol_cust_df.columns and "客户单号" in bol_cust_df.columns:
        for _, rr in bol_cust_df.iterrows():
            wb = _norm_waybill_str(rr.get("运单号", ""))
            cust = str(rr.get("客户单号", "")).strip()
            if wb and cust:
                cust_map[wb] = cust

    pallets = []
    for _, brow in base.iterrows():
        pid, wh = brow["托盘号"], brow["仓库代码"]
        if _is_blank(pid):
            continue

        p_wt = brow.get("托盘重量", None)
        p_vol = brow.get("托盘体积", None)

        waybills = brow.get("运单清单_list", []) or []
        waybills_disp = []
        for wb in waybills:
            wb_norm = _norm_waybill_str(wb)
            cust = cust_map.get(wb_norm, "")
            waybills_disp.append(f"{wb}({cust})" if cust else f"{wb}")

        create_date_str, create_time_str = _split_dt_to_date_time_str(
            brow.get("托盘创建日期_raw", ""),
            brow.get("托盘创建时间_raw", "")
        )

        sub_qty = df[(df["托盘号"] == pid) & (df["仓库代码"] == wh)].copy()
        sub_qty["运单号_norm"] = sub_qty["运单号"].map(_norm_waybill_str)
        qty_col_local = qty_col
        qty_map = (
            sub_qty.groupby("运单号_norm")[qty_col_local]
                   .sum(min_count=1)
                   .to_dict()
        )

        waybills_disp_qty = []
        for wb in waybills:
            wb_norm = _norm_waybill_str(wb)
            q = qty_map.get(wb_norm, None)
            if q is None or pd.isna(q):
                q_str = "-"
            else:
                q_str = str(int(q)) if abs(q - round(q)) < 1e-9 else f"{q:.2f}"
            waybills_disp_qty.append(f"{wb}({q_str})")

        sub = df_join[(df_join["托盘号"] == pid) & (df_join["仓库代码"] == wh)]
        lines_etaata, lines_etdatd, promised = [], [], []
        diffs_days = []
        for _, r in sub.iterrows():
            wb = r.get("运单号", "")
            etaata_s = r.get("ETA/ATA", pd.NA)
            etdatd_s = r.get("ETD/ATD", "")
            promise = r.get("对客承诺送仓时间", "")
            anchor = r.get("_ETAATA_date", None)
            lines_etaata.append(f"{wb}: ETA/ATA {etaata_s if not _is_blank(etaata_s) else '-'}")
            lines_etdatd.append(f"{wb}: {'' if _is_blank(etdatd_s) else str(etdatd_s)}")
            if not _is_blank(promise):
                diffs_days.append(_promise_diff_days_str(str(promise).strip(), anchor or date.today()))
                promised.append(str(promise).strip())

        readable_etaata = " ; ".join(lines_etaata) if lines_etaata else ""
        readable_etdatd = " ; ".join(lines_etdatd) if lines_etdatd else ""
        promised_set = list(dict.fromkeys([p for p in promised if p]))
        promised_str = " , ".join(promised_set)

        diff_days_str = ""
        if diffs_days:
            def keyfn(s):
                try:
                    a, _ = s.split("-", 1)
                    return int(a)
                except Exception:
                    return 10**9
            diff_days_str = sorted(diffs_days, key=keyfn)[0]

        L_in = brow.get("托盘长in", None)
        W_in = brow.get("托盘宽in", None)
        H_in = brow.get("托盘高in", None)

        pallets.append({
            "托盘号": pid,
            "仓库代码": wh,
            "托盘重量": float(p_wt) if pd.notna(p_wt) else None,
            "托盘体积": float(p_vol) if p_vol is not None else None,  # m³
            "长(in)": round(float(L_in), 2) if pd.notna(L_in) else None,
            "宽(in)": round(float(W_in), 2) if pd.notna(W_in) else None,
            "高(in)": round(float(H_in), 2) if pd.notna(H_in) else None,
            "托盘创建日期": create_date_str,
            "托盘创建时间": create_time_str,
            "运单数量": len(waybills),
            "运单清单": ", ".join(waybills_disp) if waybills_disp else "",
            "运单箱数": ", ".join(waybills_disp_qty) if waybills_disp_qty else "",
            "对客承诺送仓时间": promised_str,
            "送仓时段差值(天)": diff_days_str,
            "ETA/ATA(按运单)": readable_etaata,
            "ETD/ATD(按运单)": readable_etdatd,
        })

    out = pd.DataFrame(pallets)
    if out.empty:
        return out
    out = out[out["托盘号"].astype(str).str.strip() != ""].copy()

    for c in ["托盘体积","托盘重量","长(in)","宽(in)","高(in)"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out["托盘体积"] = out["托盘体积"].round(2)
    out["长(in)"] = out["长(in)"].round(2)
    out["宽(in)"] = out["宽(in)"].round(2)
    out["高(in)"] = out["高(in)"].round(2)

    return out

@st.cache_data(ttl=300)
def load_shipped_pallet_ids(_bust=0):
    try:
        ws = client.open(SHEET_SHIP_TRACKING).sheet1
    except SpreadsheetNotFound:
        return set()
    vals = _safe_get_all_values(ws)
    if not vals: return set()
    raw_header = vals[0]
    norm_header = _norm_header(raw_header)
    norm_header_lower = [h.lower() for h in norm_header]
    candidates = ["托盘号", "托盘编号", "托盘id", "palletid", "palletno", "pallet编号"]
    col_idx = None
    for name in candidates:
        n = name.replace(" ","")
        if n in norm_header: col_idx = norm_header.index(n); break
        if n.lower() in norm_header_lower: col_idx = norm_header_lower.index(n.lower()); break
    if col_idx is None: return set()
    shipped = set()
    for r in vals[1:]:
        if len(r)>col_idx:
            pid = str(r[col_idx]).strip()
            if pid: shipped.add(pid)
    return shipped

@st.cache_data(ttl=300)
def load_bol_waybill_costs(_bust=0):
    try:
        ws = client.open(SHEET_BOL_DETAIL).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame()
    vals = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
    if not vals:
        return pd.DataFrame()
    header = _norm_header(vals[0])
    df = pd.DataFrame(vals[1:], columns=header) if len(vals)>1 else pd.DataFrame(columns=header)

    col_wb    = next((c for c in ["运单号","Waybill","waybill"] if c in df.columns), None)
    col_truck = next((c for c in ["卡车单号","卡车号","TruckNo","truckno","Truck","truck"] if c in df.columns), None)
    col_cost  = next((c for c in ["分摊费用","费用","Amount","amount","cost"] if c in df.columns), None)
    col_date  = next((c for c in ["ETA(到BCF)","ETA到BCF","到BCFETA","日期","Date","date"] if c in df.columns), None)
    col_cust  = next((c for c in ["客户单号","客户PO","客户PO号","客户参考号","CustomerPO","CustomerRef","Reference"] if c in df.columns), None)

    if col_wb is None or col_cost is None:
        return pd.DataFrame()

    df[col_wb] = df[col_wb].apply(_norm_waybill_str)
    df[col_cost] = df[col_cost].apply(_to_num_safe)
    if col_truck: df[col_truck] = df[col_truck].astype(str).str.strip()
    if col_cust:  df[col_cust]  = df[col_cust].astype(str).str.strip()
    if col_date:
        df["_date"] = df[col_date].apply(_parse_sheet_value_to_date)
        df[col_date] = df["_date"].apply(_fmt_date).replace("", pd.NA)

    if df.empty:
        return pd.DataFrame()

    agg_dict = {col_cost: "sum"}
    if col_truck:
        agg_dict[col_truck] = lambda s: ", ".join(sorted(set([x.strip() for x in s if not _is_blank(x)])))
    if col_date:
        agg_dict[col_date] = "min"
    if col_cust:
        def _first_nonblank(s):
            for x in s:
                if not _is_blank(x): return x
            return ""
        agg_dict[col_cust] = _first_nonblank

    g = df.groupby(col_wb, dropna=False).agg(agg_dict).reset_index()

    rename_map = {col_wb:"运单号"}
    if col_truck: rename_map[col_truck] = "到BCF卡车号"
    if col_cost:  rename_map[col_cost]  = "到BCF费用"
    if col_date:  rename_map[col_date]  = "到BCF日期"
    if col_cust:  rename_map[col_cust]  = "客户单号"
    g = g.rename(columns=rename_map)
    for c in ["运单号","客户单号","到BCF日期","到BCF卡车号","到BCF费用"]:
        if c not in g.columns: g[c] = pd.NA
    g["到BCF费用"] = pd.to_numeric(g["到BCF费用"], errors="coerce").round(2)
    return g[["运单号","客户单号","到BCF日期","到BCF卡车号","到BCF费用"]]

@st.cache_data(ttl=300)
def load_ship_tracking_raw(_bust=0):
    try:
        ws = client.open(SHEET_SHIP_TRACKING).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame()
    vals = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
    if not vals: return pd.DataFrame()
    header = _norm_header(vals[0])
    df = pd.DataFrame(vals[1:], columns=header) if len(vals)>1 else pd.DataFrame(columns=header)

    if "托盘号" not in df.columns:
        for c in ["托盘编号","PalletID","PalletNo","palletid","palletno"]:
            if c in df.columns: df = df.rename(columns={c:"托盘号"}); break
    if "运单清单" not in df.columns:
        for c in ["运单号清单","运单列表","Waybills","waybills"]:
            if c in df.columns: df = df.rename(columns={c:"运单清单"}); break
    if "卡车单号" not in df.columns:
        for c in ["TruckNo","truckno","Truck","truck","卡车号"]:
            if c in df.columns: df = df.rename(columns={c:"卡车单号"}); break
    if "分摊费用" not in df.columns:
        for c in ["费用","Amount","amount","cost"]:
            if c in df.columns: df = df.rename(columns={c:"分摊费用"}); break
    if "日期" not in df.columns:
        for c in ["Date","date"]:
            if c in df.columns: df = df.rename(columns={c:"日期"}); break

    df["托盘号"]   = df.get("托盘号","").astype(str).str.strip()
    df["卡车单号"] = df.get("卡车单号","").astype(str).str.strip()
    df["分摊费用"] = df.get("分摊费用","").apply(_to_num_safe)
    df["日期_raw"] = df.get("日期","")
    df["_date"]    = df["日期_raw"].apply(_parse_sheet_value_to_date)
    df["日期"]     = df["_date"].apply(_fmt_date).replace("", pd.NA)
    df["运单清单"] = df.get("运单清单","")
    return df[["托盘号","运单清单","卡车单号","分摊费用","日期"]]

@st.cache_data(ttl=300)
def load_customer_refs_from_arrivals(_bust=0):
    try:
        ws = client.open(SHEET_ARRIVALS_NAME).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame(columns=["运单号","客户单号"])
    vals = _safe_get_all_values(ws)
    if not vals:
        return pd.DataFrame(columns=["运单号","客户单号"])
    header = _norm_header(vals[0])
    df = pd.DataFrame(vals[1:], columns=header)
    cust_col = next((c for c in ["客户单号","客户PO","客户PO号","客户参考号","CustomerPO","CustomerRef","Reference"] if c in df.columns), None)
    wb_col   = next((c for c in ["运单号","Waybill","waybill"] if c in df.columns), None)
    if not cust_col or not wb_col:
        return pd.DataFrame(columns=["运单号","客户单号"])
    out = df[[wb_col, cust_col]].copy()
    out[wb_col] = out[wb_col].apply(_norm_waybill_str)
    out[cust_col] = out[cust_col].astype(str).str.strip()
    out = out.rename(columns={wb_col:"运单号", cust_col:"客户单号"})
    out = out[out["运单号"]!=""].drop_duplicates(subset=["运单号"])
    return out[["运单号","客户单号"]]

@st.cache_data(ttl=300)
def load_customer_refs_from_pallet(_bust=0):
    try:
        ws = client.open(SHEET_PALLET_DETAIL).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame(columns=["运单号","客户单号"])
    vals = _safe_get_all_values(ws)
    if not vals:
        return pd.DataFrame(columns=["运单号","客户单号"])
    header = _norm_header(vals[0])
    df = pd.DataFrame(vals[1:], columns=header)
    cust_col = next((c for c in ["客户单号","客户PO","客户PO号","客户参考号","CustomerPO","CustomerRef","Reference"] if c in df.columns), None)
    wb_col   = next((c for c in ["运单号","Waybill","waybill"] if c in df.columns), None)
    if not cust_col or not wb_col:
        return pd.DataFrame(columns=["运单号","客户单号"])
    out = df[[wb_col, cust_col]].copy()
    out[wb_col] = out[wb_col].apply(_norm_waybill_str)
    out[cust_col] = out[cust_col].astype(str).str.strip()
    out = out.rename(columns={wb_col:"运单号", cust_col:"客户单号"})
    out = out[out["运单号"]!=""].drop_duplicates(subset=["运单号"])
    return out[["运单号","客户单号"]]

# ===================== 运单增量构建 =====================
def _extract_pure_waybills(mixed: str) -> list[str]:
    """
    从《发货追踪》的“运单清单”字段中提取纯运单号列表。
    """
    if _is_blank(mixed):
        return []
    s = str(mixed).strip()
    s_no_paren = re.sub(r"[\(\（][\s\S]*?[\)\）]", "", s, flags=re.DOTALL).strip()
    if not s_no_paren:
        return []
    parts = re.split(r"[,\，;\；、\|\/\s]+", s_no_paren)
    out = []
    for p in parts:
        if not p:
            continue
        token = _norm_waybill_str(p)
        if not token:
            continue
        if token.upper().startswith("IP"):
            continue
        if not (re.search(r"[A-Za-z]", token) and re.search(r"\d", token) and len(token) >= 8):
            continue
        out.append(token)
    return out

def build_waybill_delta():
    """
    聚合到“运单粒度”的增量数据，供部分更新《运单全链路汇总》
    """
    arrivals = load_arrivals_df(_bust=_get_bust("arrivals"))
    bol      = load_bol_waybill_costs(_bust=_get_bust("bol_detail"))
    track    = load_ship_tracking_raw(_bust=_get_bust("ship_tracking"))

    wb_from_track = set()
    for _, r in track.iterrows():
        for wb in _extract_pure_waybills(r.get("运单清单", "")):
            if wb: wb_from_track.add(wb)

    if not wb_from_track:
        return pd.DataFrame(columns=[
            "运单号","客户单号","仓库代码","收费重","体积",
            "发出(ETD/ATD)","到港(ETA/ATA)",
            "到BCF日期","发走日期","到仓日期",
            "到BCF卡车号","到BCF费用",
            "发走卡车号","发走费用"
        ])

    arrivals = arrivals[arrivals["运单号"].isin(wb_from_track)].copy()
    if not bol.empty:
        bol = bol[bol["运单号"].isin(wb_from_track)].copy()

    weight_map = dict(zip(
        arrivals["运单号"],
        pd.to_numeric(arrivals["收费重"], errors="coerce")
    ))

    wb2_cost, wb2_trucks, wb2_date = {}, {}, {}
    for _, r in track.iterrows():
        waybills = _extract_pure_waybills(r.get("运单清单", ""))
        waybills = [wb for wb in waybills if wb in wb_from_track]
        if not waybills:
            continue
        pallet_cost = _to_num_safe(r.get("分摊费用"))
        truck_no    = r.get("卡车单号", "")
        dt_str      = r.get("日期", None)
        dt_obj      = _parse_sheet_value_to_date(dt_str) if not _is_blank(dt_str) else None

        weights = [weight_map.get(wb, None) for wb in waybills]
        valid   = [w for w in weights if w and w > 0]
        if valid and sum(valid) > 0:
            total_w = sum(valid)
            shares  = [(w/total_w if (w and w > 0) else 0.0) for w in weights]
        else:
            shares  = [1.0/len(waybills)] * len(waybills)

        if pallet_cost is not None:
            for wb, s in zip(waybills, shares):
                wb2_cost[wb] = wb2_cost.get(wb, 0.0) + pallet_cost * s
        if truck_no:
            for wb in waybills:
                wb2_trucks.setdefault(wb, set()).add(truck_no)
        if dt_obj:
            for wb in waybills:
                if (wb not in wb2_date) or (dt_obj < wb2_date[wb]):
                    wb2_date[wb] = dt_obj

    out = pd.DataFrame({"运单号": sorted(wb_from_track)})

    if not arrivals.empty:
        arr2 = arrivals[["运单号","仓库代码","收费重","体积","ETD/ATD","ETA/ATA","预计到仓时间（日）"]].copy()
        arr2 = arr2.rename(columns={
            "ETD/ATD": "发出(ETD/ATD)",
            "ETA/ATA": "到港(ETA/ATA)",
            "预计到仓时间（日）": "到仓日期"
        })
        out = out.merge(arr2, on="运单号", how="left")
    else:
        out["仓库代码"] = pd.NA
        out["收费重"] = pd.NA
        out["体积"]   = pd.NA
        out["发出(ETD/ATD)"] = pd.NA
        out["到港(ETA/ATA)"] = pd.NA
        out["到仓日期"]       = pd.NA

    cust_bol = bol[["运单号","客户单号"]] if (not bol.empty and "客户单号" in bol.columns) \
               else pd.DataFrame(columns=["运单号","客户单号"])
    cust_pal = load_customer_refs_from_pallet(_bust=_get_bust("pallet_detail"))
    cust_arr = load_customer_refs_from_arrivals(_bust=_get_bust("arrivals"))
    for d in (cust_pal, cust_arr):
        if not d.empty:
            d.drop_duplicates(subset=["运单号"], inplace=True)
            d["运单号"] = d["运单号"].map(_norm_waybill_str)
    cust_all = pd.concat([cust_bol.assign(_pri=1), cust_pal.assign(_pri=2), cust_arr.assign(_pri=3)], ignore_index=True)
    if not cust_all.empty:
        cust_all = cust_all[cust_all["运单号"].isin(wb_from_track)]
        cust_all = cust_all[~cust_all["客户单号"].isna() & (cust_all["客户单号"].astype(str)!="")]
        cust_all = (cust_all.sort_values(["运单号","_pri"])
                            .drop_duplicates(subset=["运单号"], keep="first")[["运单号","客户单号"]])
        out = out.merge(cust_all, on="运单号", how="left")
    else:
        out["客户单号"] = pd.NA

    if not bol.empty:
        out = out.merge(bol[["运单号","到BCF日期","到BCF卡车号","到BCF费用"]], on="运单号", how="left")
    else:
        for c in ["到BCF日期","到BCF卡车号","到BCF费用"]:
            out[c] = pd.NA

    out["发走费用"]   = out["运单号"].map(lambda wb: round(wb2_cost.get(wb, 0.0), 2) if wb in wb2_cost else pd.NA)
    out["发走卡车号"] = out["运单号"].map(lambda wb: ", ".join(sorted(wb2_trucks.get(wb, []))) if wb in wb2_trucks else pd.NA)
    out["发走日期"]   = out["运单号"].map(lambda wb: _fmt_date(wb2_date.get(wb)) if wb in wb2_date else pd.NA)

    out["收费重"]   = pd.to_numeric(out["收费重"], errors="coerce")
    out["体积"]     = pd.to_numeric(out["体积"], errors="coerce").round(2)
    out["到BCF费用"] = pd.to_numeric(out["到BCF费用"], errors="coerce").round(2)
    out["发走费用"]  = pd.to_numeric(out["发走费用"], errors="coerce").round(2)

    final_cols = [
        "运单号","客户单号","仓库代码","收费重","体积",
        "发出(ETD/ATD)","到港(ETA/ATA)",
        "到BCF日期","发走日期","到仓日期",
        "到BCF卡车号","到BCF费用",
        "发走卡车号","发走费用"
    ]
    for c in final_cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out[final_cols]

MANAGED_COLS = [
    "运单号","客户单号","发出(ETD/ATD)","到港(ETA/ATA)",
    "到BCF日期","发走日期","到仓日期","到BCF卡车号","到BCF费用",
    "发走卡车号","发走费用","仓库代码","收费重","体积"
]

def _is_effective(v):
    if v is None: return False
    if isinstance(v, float) and pd.isna(v): return False
    if isinstance(v, str) and v.strip() == "": return False
    return True

def _to_jsonable_cell(v):
    try:
        if v is None or pd.isna(v):
            return ""
    except Exception:
        if v is None:
            return ""
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return "" if np.isnan(v) or np.isinf(v) else float(v)
    if isinstance(v, float):
        return "" if (math.isnan(v) or math.isinf(v)) else v
    return v if isinstance(v, (str, int, float, bool)) else str(v)

def upsert_waybill_summary_partial(df_delta: pd.DataFrame):
    """
    只对 MANAGED_COLS 做“定点值更新”或“追加新行”，并加入『写入策略』保护人工修改：
      - blank_only：仅当目标单元格为空时才写
      - merge_set：把新旧字符串按分隔符合并去重
      - default  ：有有效值就写、空值不写
    """
    WRITE_POLICY = {
        "到仓日期": "blank_only",
        "发走日期": "blank_only",
        "到BCF日期": "blank_only",
        "到BCF费用": "blank_only",
        "发走费用": "blank_only",
        "到BCF卡车号": "merge_set",
        "发走卡车号": "merge_set",
        "仓库代码": "blank_only",
        "客户单号": "blank_only",
    }
    MERGE_SEP = ","

    def _cell_blank(x):
        return (x is None) or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip()=="")

    def _merge_set(old, new):
        def toks(s):
            if _cell_blank(s): return []
            parts = re.split(r"[,\，;\；\|/ ]+", str(s).strip())
            return [p for p in parts if p]
        seen = []
        for t in toks(old) + toks(new):
            if t not in seen:
                seen.append(t)
        return MERGE_SEP.join(seen)

    try:
        ws = client.open(SHEET_WB_SUMMARY).sheet1
    except SpreadsheetNotFound:
        st.error(f"找不到工作表「{SHEET_WB_SUMMARY}」。请先创建并在第1行写入表头（至少包含：运单号）。")
        return False

    vals = _safe_get_all_values(ws, value_render_option="UNFORMATTED_VALUE", date_time_render_option="SERIAL_NUMBER")
    if not vals or not vals[0]:
        st.error("『运单全链路汇总』为空且无表头。请先在第一行写好表头（至少包含：运单号）。")
        return False

    header = list(vals[0])
    if "运单号" not in header:
        st.error("『运单全链路汇总』缺少“运单号”表头，无法更新。")
        return False

    df_delta = df_delta.copy()
    if "运单号" not in df_delta.columns:
        st.error("增量数据缺少“运单号”。")
        return False
    df_delta["运单号"] = df_delta["运单号"].map(_norm_waybill_str)

    missing_cols = [c for c in MANAGED_COLS if c not in header]
    if missing_cols:
        ws.update(f"{ws.title}!1:1", [header + missing_cols], value_input_option="USER_ENTERED")
        header = header + missing_cols

    exist_df = pd.DataFrame(vals[1:], columns=header) if len(vals) > 1 else pd.DataFrame(columns=header)
    if "运单号" not in exist_df.columns:
        exist_df["运单号"] = ""
    exist_df["运单号"] = exist_df["运单号"].map(_norm_waybill_str)
    exist_df["_rowno"] = np.arange(2, 2 + len(exist_df))

    idx_exist = exist_df.set_index("运单号", drop=False)
    idx_delta = df_delta.set_index("运单号", drop=False)

    common  = idx_delta.index.intersection(idx_exist.index)
    new_ids = list(idx_delta.index.difference(idx_exist.index))

    updates = []

    for col in MANAGED_COLS:
        if col == "运单号":
            continue
        if col not in header or col not in idx_delta.columns:
            continue
        col_idx = header.index(col) + 1

        rows_payload = []
        policy = WRITE_POLICY.get(col, "default")

        for wb in common:
            new_v = idx_delta.loc[wb, col]
            if not _is_effective(new_v):
                continue
            rno = int(idx_exist.loc[wb, "_rowno"])
            old_v = idx_exist.loc[wb, col] if col in idx_exist.columns else ""

            if policy == "blank_only":
                if not _cell_blank(old_v):
                    continue
                write_v = new_v
            elif policy == "merge_set":
                write_v = _merge_set(old_v, new_v)
            else:
                write_v = new_v

            rows_payload.append((rno, [_to_jsonable_cell(write_v)]))

        if not rows_payload:
            continue

        rows_payload.sort(key=lambda x: x[0])
        s = p = None
        buf = []
        for r, val in rows_payload:
            if s is None:
                s = p = r; buf = [val]
            elif r == p + 1:
                p = r; buf.append(val)
            else:
                a1_start = gspread.utils.rowcol_to_a1(s, col_idx)
                a1_end   = gspread.utils.rowcol_to_a1(p, col_idx)
                updates.append({"range": f"{ws.title}!{a1_start}:{a1_end}", "values": buf})
                s = p = r; buf = [val]
        if s is not None:
            a1_start = gspread.utils.rowcol_to_a1(s, col_idx)
            a1_end   = gspread.utils.rowcol_to_a1(p, col_idx)
            updates.append({"range": f"{ws.title}!{a1_start}:{a1_end}", "values": buf})

    if updates:
        ws.spreadsheet.values_batch_update(body={"valueInputOption": "USER_ENTERED", "data": updates})

    if new_ids:
        cols_out = [c for c in header if c in MANAGED_COLS]
        if "运单号" not in cols_out:
            cols_out = ["运单号"] + cols_out

        new_rows = []
        for wb in new_ids:
            row_dict = {c: "" for c in header}
            row_dict["运单号"] = wb
            for c in MANAGED_COLS:
                if c == "运单号" or c not in header:
                    continue
                if c in idx_delta.columns:
                    v = idx_delta.loc[wb, c]
                    if _is_effective(v):
                        if WRITE_POLICY.get(c) == "merge_set":
                            v = v  # 新行是空，等同直接写
                        row_dict[c] = _to_jsonable_cell(v)
            new_rows.append([row_dict.get(c, "") for c in header])

        if new_rows:
            ws.append_rows(new_rows, value_input_option="USER_ENTERED")

    return True

# ========= UI：仅启用“按托盘发货” + 「按卡车回填到仓日期」 =========
st.set_page_config(page_title="BCF 发货调度（仅托盘）", layout="wide")
st.title("🚚 BCF 发货调度（仅托盘）")

# ======= 上传按钮放大 + 高亮样式（全局）=======
st.markdown("""
    <style>
    div.stButton > button[kind="secondary"] {
        font-size: 28px !important;
        font-weight: 900 !important;
        padding: 0.8em 1.6em !important;
        background-color: #ff9800 !important;
        color: white !important;
        border-radius: 10px !important;
        border: 3px solid #e65100 !important;
    }
    </style>
""", unsafe_allow_html=True)

tab1, tab2 = st.tabs(["按托盘发货","按卡车回填到仓日期"])

with tab1:
    # 刷新
    c1,_ = st.columns([1,6])
    with c1:
        if st.button("🔄 刷新托盘数据缓存", key="btn_refresh_pallet"):
            # 局部 bust 托盘明细，不清全站
            _bust("pallet_detail")
            st.rerun()

    # 可选：先读依赖表，再注入到托盘读取，减少重复读
    arrivals_df = load_arrivals_df(_bust=_get_bust("arrivals"))
    bol_df      = load_bol_waybill_costs(_bust=_get_bust("bol_detail"))
    pallet_df   = load_pallet_detail_df(arrivals_df=arrivals_df, bol_cost_df=bol_df, _bust=_get_bust("pallet_detail"))

    if pallet_df.empty:
        st.warning("未从『托盘明细表』读取到数据，请检查表名/权限/表头。")
        st.stop()

    shipped_pallets = load_shipped_pallet_ids(_bust=_get_bust("ship_tracking"))
    if shipped_pallets:
        pallet_df = pallet_df[~pallet_df["托盘号"].isin(shipped_pallets)]

    if pallet_df.empty:
        st.info("当前可发货的托盘为空（可能都已记录在『发货追踪』）。")
        st.stop()

    # 仓库筛选
    wh_opts = ["（全部）"] + sorted([w for w in pallet_df["仓库代码"].dropna().unique() if str(w).strip()])
    wh_pick = st.selectbox("选择仓库代码（可选）", options=wh_opts, key="wh_pallet")
    if wh_pick != "（全部）":
        pallet_df = pallet_df[pallet_df["仓库代码"]==wh_pick]

    # 表格与勾选
    show_cols = [
        "托盘号","仓库代码","托盘重量","长(in)","宽(in)","高(in)","托盘体积",
        "托盘创建日期","托盘创建时间",
        "运单数量","运单清单","运单箱数",
        "对客承诺送仓时间","送仓时段差值(天)",
        "ETA/ATA(按运单)","ETD/ATD(按运单)"
    ]
    for c in show_cols:
        if c not in pallet_df.columns:
            pallet_df[c] = ""

    disp_df = pallet_df.copy().reset_index(drop=True)
    for c in ["托盘体积","托盘重量","长(in)","宽(in)","高(in)"]:
        disp_df[c] = pd.to_numeric(disp_df.get(c, pd.Series()), errors="coerce")
    disp_df["托盘体积"] = disp_df["托盘体积"].round(2)
    disp_df["长(in)"] = disp_df["长(in)"].round(2)
    disp_df["宽(in)"] = disp_df["宽(in)"].round(2)
    disp_df["高(in)"] = disp_df["高(in)"].round(2)

    if "选择" not in disp_df.columns:
        disp_df["选择"] = False
    cols_order = ["选择"] + show_cols

    if "sel_locked" not in st.session_state:
        st.session_state.sel_locked = False
    if "locked_df" not in st.session_state:
        st.session_state.locked_df = pd.DataFrame()

    if not st.session_state.sel_locked:
        with st.form("pick_pallets_form", clear_on_submit=False):
            edited_pal = st.data_editor(
                disp_df[cols_order],
                hide_index=True,
                use_container_width=True,
                height=500,
                column_config={"选择": st.column_config.CheckboxColumn("选择")},
                disabled=[c for c in show_cols],
                key="pallet_select_editor"
            )
            submitted = st.form_submit_button("🔒 锁定选择并进入计算")
        if submitted:
            selected_pal = edited_pal[edited_pal["选择"]==True].copy()
            if len(selected_pal) == 0:
                st.warning("请至少勾选一个托盘再点击『锁定选择并进入计算』。")
                st.stop()
            st.session_state.locked_df = selected_pal.reset_index(drop=True)
            st.session_state.all_snapshot_df = disp_df[cols_order].copy()
            st.session_state.sel_locked = True
            st.rerun()

    if st.session_state.sel_locked:
        st.success("✅ 已锁定托盘选择")
        if st.button("🔓 重新选择"):
            st.session_state.sel_locked = False
            st.session_state.locked_df = pd.DataFrame()
            st.rerun()

        selected_pal = st.session_state.locked_df.copy()
        locked_ids = set(selected_pal["托盘号"].astype(str))
        others_df = disp_df[~disp_df["托盘号"].astype(str).isin(locked_ids)].copy()
        if "选择" in others_df.columns:
            others_df["选择"] = False

        left, right = st.columns([2, 2], gap="large")
        with left:
            st.markdown("**📦 已锁定托盘（用于计算）**")
            st.dataframe(
                selected_pal[cols_order],
                use_container_width=True,
                height=320
            )
            st.caption(f"已锁定数量：{len(selected_pal)}")
        with right:
            st.markdown("**🗂 其他托盘（未锁定，仅查看）**")
            st.dataframe(
                others_df[cols_order],
                use_container_width=True,
                height=320
            )
            st.caption(f"未锁定数量：{len(others_df)}")

        sel_count = int(len(selected_pal))
        sel_vol_sum = pd.to_numeric(selected_pal.get("托盘体积", pd.Series()), errors="coerce").sum()
        m1, m2 = st.columns(2)
        with m1: st.metric("已选择托盘数", sel_count)
        with m2: st.metric("选中体积合计（CBM）", round(float(sel_vol_sum or 0.0), 2))

        if sel_count == 0:
            st.info("当前没有锁定的托盘。点击『重新选择』返回。")
            st.stop()

        st.subheader("🧾 车次信息（托盘维度分摊）")
        cc1, cc2 = st.columns([2,2])
        with cc1:
            pallet_truck_no = st.text_input("卡车单号（必填）", key="pallet_truck_no")
        with cc2:
            pallet_total_cost = st.number_input("本车总费用（必填）", min_value=0.0, step=1.0, format="%.2f", key="pallet_total_cost")

        if not pallet_truck_no or pallet_total_cost <= 0:
            st.info("请填写卡车单号与本车总费用。")
            st.stop()

        selected_pal["托盘重量"] = pd.to_numeric(selected_pal["托盘重量"], errors="coerce")
        weights = selected_pal["托盘重量"]
        if weights.isna().any() or (weights.dropna() <= 0).any():
            st.error("所选托盘存在缺失或非正的『托盘重量』，无法分摊。请先在『托盘明细表』修正。")
            st.stop()

        wt_sum = float(weights.sum())
        if wt_sum <= 0:
            st.error("总托盘重量为 0，无法分摊。")
            st.stop()

        selected_pal["分摊比例"] = weights / wt_sum
        selected_pal["分摊费用_raw"] = selected_pal["分摊比例"] * float(pallet_total_cost)
        selected_pal["分摊费用"] = selected_pal["分摊费用_raw"].round(2)
        diff_cost = round(float(pallet_total_cost) - selected_pal["分摊费用"].sum(), 2)
        if abs(diff_cost) >= 0.01:
            selected_pal.loc[selected_pal.index[-1], "分摊费用"] += diff_cost

        upload_df = selected_pal.copy()
        upload_df["卡车单号"] = pallet_truck_no
        upload_df["总费用"] = round(float(pallet_total_cost), 2)
        upload_df["分摊比例"] = (upload_df["分摊比例"]*100).round(2).astype(str) + "%"
        upload_df["分摊费用"] = upload_df["分摊费用"].map(lambda x: f"{x:.2f}")
        upload_df["总费用"] = upload_df["总费用"].map(lambda x: f"{x:.2f}")
        upload_df["托盘体积"] = pd.to_numeric(upload_df.get("托盘体积", pd.Series()), errors="coerce").round(2)

        preview_cols_pal = [
            "卡车单号","仓库代码","托盘号","托盘重量","长(in)","宽(in)","高(in)","托盘体积",
            "托盘创建日期","托盘创建时间",
            "运单数量","运单清单",
            "对客承诺送仓时间","送仓时段差值(天)",
            "ETA/ATA(按运单)","ETD/ATD(按运单)",
            "分摊比例","分摊费用","总费用"
        ]
        for c in preview_cols_pal:
            if c not in upload_df.columns:
                upload_df[c] = ""

        st.subheader("✅ 上传预览（托盘 → 发货追踪）")
        st.dataframe(upload_df[preview_cols_pal], use_container_width=True, height=360)

        st.markdown("""
        **分摊比例计算公式：** 每个托盘的分摊比例 = 该托盘重量 ÷ 所有选中托盘重量总和  
        **分摊费用计算公式：** 每个托盘的分摊费用 = 分摊比例 × 本车总费用  
        （最后一托盘自动调整几分钱差额，确保总额=本车总费用）
        """)

        if st.button("📤 追加上传到『发货追踪』", key="btn_upload_pallet"):
            try:
                ss = client.open(SHEET_SHIP_TRACKING); ws_track = ss.sheet1
            except SpreadsheetNotFound:
                st.error(f"找不到工作表「{SHEET_SHIP_TRACKING}」。请先在 Google Drive 中创建，并设置第一行表头。")
                st.stop()

            exist = _safe_get_all_values(ws_track)
            if not exist:
                st.error("目标表为空且无表头。请先在第一行写好表头（标题行）。")
                st.stop()

            header_raw = exist[0]
            header_norm = _norm_header(header_raw)
            header_norm_lower = [h.lower() for h in header_norm]
            need_ok = any(n in header_norm for n in ["托盘号","托盘编号"]) or \
                      any(n in header_norm_lower for n in ["palletid","palletno","pallet编号"])
            if not need_ok:
                st.error("『发货追踪』缺少“托盘号”列（或等价列如 PalletID/PalletNo）。请先在目标表增加该列。")
                st.stop()

            tmp = upload_df.copy()
            if ("日期" in header_raw) and ("日期" not in tmp.columns):
                tmp["日期"] = datetime.today().strftime("%Y-%m-%d")

            for col in header_raw:
                if col not in tmp.columns:
                    tmp[col] = ""
            rows = tmp.reindex(columns=header_raw).fillna("").values.tolist()

            ws_track.append_rows(rows, value_input_option="USER_ENTERED")
            st.success(f"已上传 {len(rows)} 条到『{SHEET_SHIP_TRACKING}』。卡车单号：{pallet_truck_no}")

            # === 上传成功后：仅局部刷新，不清全站缓存 ===
            _bust("ship_tracking")
            _ = load_ship_tracking_raw(_bust=_get_bust("ship_tracking"))

            st.info("正在更新『运单全链路汇总』（只含『发货追踪』里的运单；仅更新指定列）…")
            try:
                df_delta = build_waybill_delta()
                if df_delta.empty:
                    st.warning("没有可更新的数据（检查到仓/发货/自提表）。")
                else:
                    ok = upsert_waybill_summary_partial(df_delta)
                    if ok:
                        _bust("wb_summary")
                        _ = load_waybill_summary_df(_bust=_get_bust("wb_summary"))
                        st.success(f"已更新/新增 {len(df_delta)} 条到『{SHEET_WB_SUMMARY}』。")
                    else:
                        st.warning("未能写入『运单全链路汇总』：请先创建该表并确保首行包含“运单号”列。")
            except Exception as e:
                st.warning(f"更新『运单全链路汇总』失败：{e}")

            st.session_state.sel_locked = False
            st.session_state.locked_df = pd.DataFrame()
            st.session_state.pop("pallet_select_editor", None)
            st.rerun()

with tab2:
    st.subheader("🚚 按卡车回填到仓日期（先选仓库 → 再选卡车）")

    @st.cache_data(ttl=300)
    def load_waybill_summary_df(_bust=0):
        try:
            ws = client.open(SHEET_WB_SUMMARY).sheet1
        except SpreadsheetNotFound:
            st.error(f"找不到工作表「{SHEET_WB_SUMMARY}」。")
            return pd.DataFrame(), None, []
        vals = _safe_get_all_values(
            ws,
            value_render_option="UNFORMATTED_VALUE",
            date_time_render_option="SERIAL_NUMBER"
        )
        if not vals:
            st.warning("『运单全链路汇总』为空。")
            return pd.DataFrame(), ws, []

        header_raw = vals[0]
        df = pd.DataFrame(vals[1:], columns=header_raw) if len(vals) > 1 else pd.DataFrame(columns=header_raw)

        def pick(colnames, cands):
            for c in cands:
                if c in colnames:
                    return c
            return None

        col_wb   = pick(df.columns, ["运单号","Waybill"])
        col_wh   = pick(df.columns, ["仓库代码","仓库"])
        col_trk  = pick(df.columns, ["发走卡车号","发走车号","发走卡车","卡车号","TruckNo","Truck"])
        col_ship = pick(df.columns, ["发走日期","发货日期","出仓日期"])
        col_eta  = pick(df.columns, ["到仓日期","到仓日","到仓(wh)"])

        if col_wb   is None: df["运单号"]   = ""; col_wb   = "运单号"
        if col_wh   is None: df["仓库代码"] = ""; col_wh   = "仓库代码"
        if col_trk  is None: df["发走卡车号"] = ""; col_trk  = "发走卡车号"
        if col_ship is None: df["发走日期"]  = ""; col_ship = "发走日期"
        if col_eta  is None: df["到仓日期"]  = ""; col_eta  = "到仓日期"

        df_work = df.rename(columns={
            col_wb: "运单号",
            col_wh: "仓库代码",
            col_trk: "发走卡车号",
            col_ship: "发走日期",
            col_eta: "到仓日期",
        }).copy()

        df_work["_rowno"] = np.arange(2, 2 + len(df_work))
        df_work["_发走日期_dt"] = df_work["发走日期"].apply(_parse_sheet_value_to_date)
        df_work["_到仓日期_dt"] = df_work["到仓日期"].apply(_parse_sheet_value_to_date)

        df_work["仓库代码"] = df_work["仓库代码"].astype(str).str.strip()
        df_work["发走卡车号"] = df_work["发走卡车号"].astype(str).str.strip()

        return df_work, ws, header_raw

    df_sum, ws_sum, header_raw = load_waybill_summary_df(_bust=_get_bust("wb_summary"))
    if ws_sum is None or df_sum.empty:
        st.stop()

    st.subheader("筛选条件")

    wh_all = sorted([w for w in df_sum["仓库代码"].astype(str).unique() if w.strip()])
    wh_pick = st.multiselect("仓库代码（先选这里）", options=wh_all, placeholder="选择一个或多个仓库…")

    if wh_pick:
        df_wh = df_sum[df_sum["仓库代码"].isin(wh_pick)].copy()
    else:
        df_wh = df_sum.copy()

    truck_opts = sorted([t for t in df_wh["发走卡车号"].astype(str).unique() if t.strip()])
    if truck_opts:
        trucks_pick = st.multiselect(
            "卡车单号（从所选仓库派生）",
            options=truck_opts,
            placeholder="选择要批量回填的车次…"
        )
    else:
        st.info("当前仓库下没有可选的卡车单号。")
        trucks_pick = []

    df_for_dates = df_wh.copy()
    if trucks_pick:
        df_for_dates = df_for_dates[df_for_dates["发走卡车号"].astype(str).isin(trucks_pick)]

    valid_ship_dates = df_for_dates.loc[df_for_dates["_发走日期_dt"].notna(), "_发走日期_dt"]
    if not valid_ship_dates.empty:
        dmin, dmax = valid_ship_dates.min(), valid_ship_dates.max()
        default_start = dmin
        default_end = dmax if dmax >= dmin else dmin
        r1, r2 = st.date_input(
            "按发走日期筛选范围",
            value=(default_start, default_end),
            min_value=dmin, max_value=max(dmax, dmin)
        )
    else:
        r1 = r2 = None
        st.caption("未检索到可用的『发走日期』范围（所选条件可能没有日期数据）。")

    only_blank = st.checkbox("仅填空白到仓日期", value=True)

    filt = pd.Series(True, index=df_sum.index)
    if wh_pick:
        filt &= df_sum["仓库代码"].isin(wh_pick)
    if trucks_pick:
        filt &= df_sum["发走卡车号"].astype(str).isin(trucks_pick)
    if r1 and r2:
        filt &= df_sum["_发走日期_dt"].between(r1, r2)
    if only_blank:
        filt &= df_sum["_到仓日期_dt"].isna()

    df_target = df_sum.loc[filt].copy()

    st.markdown(f"**匹配到 {len(df_target)} 条运单**")
    st.dataframe(
        df_target[["运单号","仓库代码","发走卡车号","发走日期","到仓日期"]]
            .sort_values(["仓库代码","发走卡车号","运单号"]),
        use_container_width=True, height=360
    )

    st.divider()

    today = date.today()
    fill_date = st.date_input("填充到仓日期（批量）", value=today)

    def _get_google_credentials():
        if "gcp_service_account" in st.secrets:
            sa_info = st.secrets["gcp_service_account"]
            return Credentials.from_service_account_info(sa_info, scopes=SCOPES)
        else:
            return Credentials.from_service_account_file("service_accounts.json", scopes=SCOPES)

    def _write_arrival_date(rows_idx, date_to_fill: date):
        col_idx_1based = None
        for i, h in enumerate(header_raw):
            if h.replace(" ", "") in ["到仓日期", "到仓日", "到仓(wh)"]:
                col_idx_1based = i + 1
                break
        if col_idx_1based is None:
            st.error("目标表缺少『到仓日期』列。请先在表头新增该列后重试。")
            return False
        if not rows_idx:
            return True

        rows = sorted(int(r) for r in rows_idx)
        ranges = []
        s = p = rows[0]
        for r in rows[1:]:
            if r == p + 1:
                p = r
            else:
                ranges.append((s, p))
                s = p = r
        ranges.append((s, p))

        try:
            creds = _get_google_credentials()
            service = build("sheets", "v4", credentials=creds, cache_discovery=False)
            spreadsheet_id = ws_sum.spreadsheet.id
            sheet_title = ws_sum.title

            date_str = date_to_fill.strftime("%Y-%m-%d")

            batch_size = 200
            for i in range(0, len(ranges), batch_size):
                sub = ranges[i:i + batch_size]
                data = []
                for r1_, r2_ in sub:
                    a1_start = gspread.utils.rowcol_to_a1(r1_, col_idx_1based)
                    a1_end   = gspread.utils.rowcol_to_a1(r2_, col_idx_1based)
                    a1_range = f"{sheet_title}!{a1_start}:{a1_end}"
                    values = [[date_str] for _ in range(r2_ - r1_ + 1)]
                    data.append({"range": a1_range, "values": values})

                body = {"valueInputOption": "USER_ENTERED", "data": data}
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=body
                ).execute()

            return True
        except HttpError as e:
            st.error(f"写入失败（HTTP）：{e}")
            return False
        except Exception as e:
            st.error(f"写入失败：{e}")
            return False

    left, right = st.columns([1,1])
    with left:
        st.caption("提示：先选仓库，再选卡车；可按发走日期范围过滤；勾选“仅填空白”避免覆盖已有值。")
    with right:
        if st.button("📝 批量写入到仓日期", key="btn_fill_arrival_date"):
            if df_target.empty:
                st.warning("筛选结果为空；请调整仓库/卡车/日期条件。")
            else:
                ok = _write_arrival_date(df_target["_rowno"].tolist(), fill_date)
                if ok:
                    st.success(f"已更新 {len(df_target)} 行的『到仓日期』为 {fill_date.strftime('%Y-%m-%d')}。")
                    _bust("wb_summary")
                    st.rerun()
