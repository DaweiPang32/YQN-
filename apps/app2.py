# 🚚 发货调度（严格 USSH 运单号 + 客户单号仅来自『到仓数据表』）

import streamlit as st
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound, APIError
from googleapiclient.discovery import build
from streamlit.errors import StreamlitAPIException
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta, date
import calendar
import re
import random, time
import math

# ========= 授权范围 =========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

if not st.session_state.get("_page_configured", False):
    try:
        st.set_page_config(page_title="发货调度", layout="wide")
    except StreamlitAPIException:
        pass
    st.session_state["_page_configured"] = True

# ========= 预编译正则 =========
_RE_PARENS = re.compile(r"[\(\（][\s\S]*?[\)\）]", re.DOTALL)
_RE_SPLIT = re.compile(r"[,\，;\；、\|\/\s]+")
_RE_NUM = re.compile(r'[-+]?\d+(?:\.\d+)?')
_IP_TOKEN   = re.compile(r"\bIP\d+\b", flags=re.IGNORECASE)
_SEP_INNER  = re.compile(r"[,\，;\；\|/]+")
# ✅ 严格只认 USSH + 12 位数字（大小写不敏感）
_WB_USSH_REGEX = re.compile(r"\bUSSH\d{12}\b", flags=re.IGNORECASE)

def _extract_wb_ushh_only(mixed: str) -> list[str]:
    """仅识别 USSH + 12位数字为运单号；统一输出为大写去重保序。"""
    if _is_blank(mixed):
        return []
    hits = _WB_USSH_REGEX.findall(str(mixed))
    out, seen = [], set()
    for t in hits:
        t_norm = t.upper()
        if t_norm not in seen:
            seen.add(t_norm)
            out.append(t_norm)
    return out

def _has_multi_ip_in_parens(text: str) -> bool:
    if _is_blank(text):
        return False
    s = str(text)
    def _count_in_pair(open_ch, close_ch, src):
        depth, buf, hits = 0, [], 0
        for ch in src:
            if ch == open_ch:
                if depth == 0:
                    buf = []
                depth += 1
            elif ch == close_ch and depth > 0:
                depth -= 1
                if depth == 0:
                    inner = "".join(buf)
                    if len(_IP_TOKEN.findall(inner)) >= 2:
                        hits += 1
                    buf = []
            else:
                if depth > 0:
                    buf.append(ch)
        return hits
    return (_count_in_pair("(", ")", s) > 0) or (_count_in_pair("（", "）", s) > 0)

def _normalize_ip_list_in_parens(text: str) -> str:
    """仅当括号里出现 ≥2 个 IPxxxx 时，将分隔符统一为空格；否则保持原样。"""
    if _is_blank(text):
        return ""
    s = str(text)
    if not _has_multi_ip_in_parens(s):
        return s
    def _do_for_pair(open_ch, close_ch, src):
        out, buf, depth = [], [], 0
        for ch in src:
            if ch == open_ch:
                if depth == 0:
                    buf = []
                depth += 1
                out.append(ch)
            elif ch == close_ch and depth > 0:
                depth -= 1
                inner = "".join(buf)
                if len(_IP_TOKEN.findall(inner)) >= 2:
                    inner = _SEP_INNER.sub(" ", inner)
                    inner = re.sub(r"\s{2,}", " ", inner).strip()
                out.append(inner)
                out.append(ch)
                buf = []
            else:
                if depth > 0:
                    buf.append(ch)
                else:
                    out.append(ch)
        return "".join(out)
    s = _do_for_pair("(", ")", s)
    s = _do_for_pair("（", "）", s)
    return s

def _split_tokens(s: str) -> list[str]:
    if not isinstance(s, str):
        s = str(s) if s is not None else ""
    return [t for t in _RE_SPLIT.split(s) if t]

def _remove_parens_iter(s: str) -> str:
    if not isinstance(s, str) or not s:
        return ""
    prev = None
    out = s
    while prev != out:
        prev = out
        out = re.sub(r"\([^()]*\)", "", out)
        out = re.sub(r"（[^（）]*）", "", out)
    return out

def _first_balanced_paren_content(s: str) -> str | None:
    if not isinstance(s, str) or not s:
        return None
    start = s.find("(")
    if start != -1:
        depth = 0
        for i in range(start, len(s)):
            ch = s[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return s[start+1:i].strip()
    start = s.find("（")
    if start != -1:
        depth = 0
        for i in range(start, len(s)):
            ch = s[i]
            if ch == "（":
                depth += 1
            elif ch == "）":
                depth -= 1
                if depth == 0:
                    return s[start+1:i].strip()
    return None

# ========= 客户端复用 =========
@st.cache_resource
def get_clients():
    if "gcp_service_account" in st.secrets:
        sa_info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("service_accounts.json", scopes=SCOPES)
    gc = gspread.authorize(creds)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return gc, svc

def get_gspread_client():
    gc, _ = get_clients()
    return gc

client, sheets_service = get_clients()

# ========= 表名配置 =========
SHEET_ARRIVALS_NAME   = "到仓数据表"
SHEET_PALLET_DETAIL   = "托盘明细表"
SHEET_SHIP_TRACKING   = "发货追踪"
SHEET_BOL_DETAIL      = "bol自提明细"
SHEET_WB_SUMMARY      = "运单全链路汇总"

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
def _ensure_scalar(v, *, df_name: str, key=None, col=None):
    """
    不改变原逻辑，只把“本应单值”的读取显式化。
    如果命中多行，直接抛出清晰错误，而不是让 pandas 在 int()/float() 时炸。
    """
    if isinstance(v, pd.Series):
        try:
            vals = v.tolist()
        except Exception:
            vals = str(v)
        raise ValueError(
            f"{df_name} 命中多行，无法按单值处理。key={key}, col={col}, values={vals}"
        )
    return v


def _scalar_loc(df, row_key, col, *, df_name: str):
    """
    df.loc[row_key, col] 的安全单值读取版。
    不兜底，不自动选一条，不改逻辑。
    """
    v = df.loc[row_key, col]
    return _ensure_scalar(v, df_name=df_name, key=row_key, col=col)


def _assert_unique_key(df: pd.DataFrame, key_col: str, *, df_name: str):
    """
    显式校验“本来就应该唯一”的 key。
    不做自动修复，只在脏数据时给出明确错误。
    """
    if key_col not in df.columns:
        raise ValueError(f"{df_name} 缺少关键列: {key_col}")

    key_series = df[key_col].map(lambda x: "" if _is_blank(x) else str(x).strip())
    dup_mask = key_series.ne("") & key_series.duplicated(keep=False)

    if dup_mask.any():
        dup_df = df.loc[dup_mask, [key_col]].copy()
        dup_vals = sorted(dup_df[key_col].astype(str).str.strip().unique().tolist())
        preview = dup_vals[:20]
        more = "" if len(dup_vals) <= 20 else f" ... 共 {len(dup_vals)} 个重复 key"
        raise ValueError(
            f"{df_name} 存在重复 {key_col}: {preview}{more}"
        )


def _pack_ranges_for_col(ws_title: str, col_idx_1based: int, rowvals: list[tuple[int, list]]):
    updates = []
    if not rowvals:
        return updates
    s = p = rowvals[0][0]
    buf = [rowvals[0][1]]
    for r, v in rowvals[1:]:
        if r == p + 1:
            p = r
            buf.append(v)
        else:
            a1s = gspread.utils.rowcol_to_a1(s, col_idx_1based)
            a1e = gspread.utils.rowcol_to_a1(p, col_idx_1based)
            updates.append({"range": f"{ws_title}!{a1s}:{a1e}", "values": buf})
            s = p = r
            buf = [v]
    a1s = gspread.utils.rowcol_to_a1(s, col_idx_1based)
    a1e = gspread.utils.rowcol_to_a1(p, col_idx_1based)
    updates.append({"range": f"{ws_title}!{a1s}:{a1e}", "values": buf})
    return updates

# ==== 退避重试读取 ====
def _safe_get_all_values(ws, value_render_option="UNFORMATTED_VALUE", date_time_render_option="SERIAL_NUMBER"):
    backoffs = [0.5, 1.0, 2.0, 4.0, 8.0]
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

def _with_backoff(fn, *args, **kwargs):
    delays = [0.3, 0.6, 1.2, 2.4, 4.8, 6.0]
    last_err = None
    for i, d in enumerate([0.0] + delays):
        if d > 0:
            time.sleep(d + random.random() * 0.2)
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            if ("429" in msg) or ("Quota" in msg) or ("quota" in msg):
                last_err = e
                continue
            raise
    if last_err:
        raise last_err

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

_BASE = datetime(1899, 12, 30)

def _coerce_excel_serial_sum(v):
    try:
        if isinstance(v, (int, float)) and not pd.isna(v):
            return float(v)
    except Exception:
        pass
    if isinstance(v, str):
        s = v.strip()
        s = re.sub(r'[\u00A0\u2000-\u200B]', ' ', s)
        s = s.replace(',', '.')
        nums = _RE_NUM.findall(s)
        total, ok = 0.0, False
        for n in nums:
            try:
                total += float(n); ok = True
            except Exception: pass
        if ok: return total
    if isinstance(v, (list, tuple)):
        total, ok = 0.0, False
        for x in v:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                continue
            try:
                xs = str(x).strip().replace(',', '.')
                total += float(xs); ok = True
            except Exception: pass
        if ok: return total
    return None

def _parse_sheet_value_to_date(v):
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
        except Exception: pass
    try:
        dt = pd.to_datetime(v, errors="coerce")
        if pd.isna(dt): return None
        return dt.date()
    except Exception:
        return None

def _excel_serial_to_dt(v):
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
        except Exception: pass
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

# ========= 轻量签名 =========
@st.cache_data(ttl=10)
def _sheet_row_sig(sheet_name: str, _bust=0) -> tuple[int, int]:
    try:
        ws = client.open(sheet_name).sheet1
    except SpreadsheetNotFound:
        return (0, 0)
    vals = _safe_get_all_values(ws, "UNFORMATTED_VALUE", "SERIAL_NUMBER")
    if not vals:
        return (0, 0)
    rows = len(vals)
    cols = max((len(r) for r in vals), default=0)
    return (rows, cols)

@st.cache_data(ttl=300)
def load_bol_pickup_map(_bust=0) -> dict:
    try:
        ws = client.open(SHEET_BOL_DETAIL).sheet1
    except SpreadsheetNotFound:
        return {}
    vals = _safe_get_all_values(ws, "UNFORMATTED_VALUE", "SERIAL_NUMBER")
    if not vals:
        return {}
    header = _norm_header(vals[0])
    df = pd.DataFrame(vals[1:], columns=header) if len(vals) > 1 else pd.DataFrame(columns=header)
    col_wb = next((c for c in ["运单号","waybill","Waybill"] if c in df.columns), None)
    col_pk = next((c for c in ["自提仓库","自提仓","pickup","Pickup"] if c in df.columns), None)
    if not col_wb or not col_pk:
        return {}
    df[col_wb] = df[col_wb].apply(_norm_waybill_str)
    df[col_pk] = df[col_pk].astype(str).str.strip()
    df = df[(df[col_wb] != "") & (df[col_pk] != "")]
    df = df.drop_duplicates(subset=[col_wb], keep="last")
    return dict(zip(df[col_wb], df[col_pk]))

@st.cache_data(ttl=30)
def load_arrivals_df(_bust=0) -> pd.DataFrame:
    """
    输出列：
      运单号、仓库代码、收费重、体积、ETA/ATA、ETD/ATD、对客承诺送仓时间、预计到仓时间（日）、客户单号、_ETAATA_date
    """
    try:
        ws = client.open(SHEET_ARRIVALS_NAME).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame(columns=[
            "运单号","仓库代码","收费重","体积",
            "ETA/ATA","ETD/ATD","对客承诺送仓时间","预计到仓时间（日）",
            "客户单号","_ETAATA_date"
        ])
    vals = _safe_get_all_values(ws, "UNFORMATTED_VALUE", "SERIAL_NUMBER")
    if not vals or not vals[0]:
        return pd.DataFrame(columns=[
            "运单号","仓库代码","收费重","体积",
            "ETA/ATA","ETD/ATD","对客承诺送仓时间","预计到仓时间（日）",
            "客户单号","_ETAATA_date"
        ])
    header = _norm_header(vals[0])
    df = pd.DataFrame(vals[1:], columns=header)

    if "运单号" not in df.columns: df["运单号"] = pd.NA
    if "仓库代码" not in df.columns: df["仓库代码"] = pd.NA
    if "收费重" not in df.columns: df["收费重"] = pd.NA

    vol_col = next((c for c in ["体积","CBM","体积m3","体积(m3)","体积（m3）"] if c in df.columns), None)
    if vol_col is None:
        df["体积"] = pd.NA
    else:
        df["体积"] = pd.to_numeric(df[vol_col], errors="coerce")

    etaata_src = None
    for cand in ["ETA/ATA","ETAATA"]:
        if cand in df.columns:
            etaata_src = cand; break

    if "ETD/ATD" not in df.columns: df["ETD/ATD"] = pd.NA
    if "对客承诺送仓时间" not in df.columns: df["对客承诺送仓时间"] = pd.NA

    eta_wh_col = next((c for c in ["预计到仓时间（日）","预计到仓时间(日)","预计到仓时间日"] if c in df.columns), None)
    if eta_wh_col is None:
        df["预计到仓时间（日）"] = pd.NA
        eta_wh_col = "预计到仓时间（日）"

    # ✅ 确保客户单号列存在
    if "客户单号" not in df.columns:
        df["客户单号"] = pd.NA

    # 规范化
    df["运单号"] = df["运单号"].apply(_norm_waybill_str)
    df["仓库代码"] = df["仓库代码"].astype(str).str.strip()
    df["收费重"] = pd.to_numeric(df["收费重"], errors="coerce")

    if etaata_src is not None:
        df["_ETAATA_date"] = df[etaata_src].apply(_parse_sheet_value_to_date)
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
            "客户单号",
            "_ETAATA_date"]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA
    return df[keep]

@st.cache_data(ttl=300)
def load_waybill_summary_df(_bust=0):
    try:
        ws = client.open(SHEET_WB_SUMMARY).sheet1
    except SpreadsheetNotFound:
        st.error(f"找不到工作表「{SHEET_WB_SUMMARY}」。")
        return pd.DataFrame(), None, []
    vals = _safe_get_all_values(ws, "UNFORMATTED_VALUE", "SERIAL_NUMBER")
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

@st.cache_data(ttl=300)
def load_pallet_detail_df(arrivals_df: pd.DataFrame | None = None,
                          bol_cost_df: pd.DataFrame | None = None,
                          _bust=0,
                          refresh_token: int = 0) -> pd.DataFrame:
    ws = client.open(SHEET_PALLET_DETAIL).sheet1
    vals = _safe_get_all_values(ws, "UNFORMATTED_VALUE", "SERIAL_NUMBER")
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

    df["托盘号"]   = df["托盘号"].astype(str).str.strip()
    df["仓库代码"] = df["仓库代码"].astype(str).str.strip()
    df["运单号"]   = df["运单号"].apply(_norm_waybill_str)

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

    def _first_nonblank_str_local(s):
        for x in s:
            if not _is_blank(x): return str(x).strip()
        return ""

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
        "托盘创建日期_raw": (create_date_col, _first_nonblank_str_local),
        "托盘创建时间_raw": (create_time_col, _first_nonblank_str_local),
    }
    if len_col: agg_dict["托盘长in"] = (len_col, _first_valid_num)
    if wid_col: agg_dict["托盘宽in"] = (wid_col, _first_valid_num)
    if hei_col: agg_dict["托盘高in"] = (hei_col, _first_valid_num)

    base = (
        df.groupby(["托盘号", "仓库代码"], as_index=False, dropna=False)
          .agg(**agg_dict)
    )

    arrivals = arrivals_df if arrivals_df is not None else load_arrivals_df(_bust=_get_bust("arrivals"))

    df_join = df.merge(
        arrivals[["运单号", "ETA/ATA", "ETD/ATD", "对客承诺送仓时间", "_ETAATA_date"]],
        on="运单号", how="left"
    )

    bol_cust_df = bol_cost_df if bol_cost_df is not None else load_bol_waybill_costs(_bust=_get_bust("bol_detail"))
    cust_map = {}
    if bol_cust_df is not None and not bol_cust_df.empty and "运单号" in bol_cust_df.columns and "客户单号" in bol_cust_df.columns:
        for _, rr in bol_cust_df.iterrows():
            wb = _norm_waybill_str(rr.get("运单号", ""))
            cust = str(rr.get("客户单号", "")).strip()
            if wb and cust:
                cust_map[wb] = cust

    pickup_map = load_bol_pickup_map(_bust=_get_bust("bol_detail"))

    pallets = []
    for _, brow in base.iterrows():
        pid, wh = brow["托盘号"], brow["仓库代码"]
        if _is_blank(pid):
            continue

        p_wt  = brow.get("托盘重量", None)
        p_vol = brow.get("托盘体积", None)

        waybills = brow.get("运单清单_list", []) or []
        waybills_disp = []
        for wb in waybills:
            wb_norm = _norm_waybill_str(wb)
            cust = cust_map.get(wb_norm, "")
            disp = f"{wb}({cust})" if cust else f"{wb}"
            disp = _normalize_ip_list_in_parens(disp)
            waybills_disp.append(disp)

        create_date_str, create_time_str = _split_dt_to_date_time_str(
            brow.get("托盘创建日期_raw", ""),
            brow.get("托盘创建时间_raw", "")
        )

        sub_qty = df[(df["托盘号"] == pid) & (df["仓库代码"] == wh)].copy()
        sub_qty["运单号_norm"] = sub_qty["运单号"].map(_norm_waybill_str)
        qty_map = (
            sub_qty.groupby("运单号_norm")[qty_col]
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
            promise  = r.get("对客承诺送仓时间", "")
            anchor   = r.get("_ETAATA_date", None)
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

        pickup_list = []
        pickup_list_disp = []
        for wb in waybills:
            wb_norm = _norm_waybill_str(wb)
            pk = pickup_map.get(wb_norm, "")
            if pk and str(pk).strip():
                pickup_list.append(str(pk).strip())
                pickup_list_disp.append(f"{wb}({pk})")
            else:
                pickup_list_disp.append(f"{wb}(-)")

        if pickup_list:
            uniq = sorted(set(pickup_list))
            pallet_pickup = uniq[0] if len(uniq) == 1 else "（多自提仓）"
        else:
            pallet_pickup = ""

        pallets.append({
            "托盘号": pid,
            "仓库代码": wh,
            "自提仓库(按托盘)": pallet_pickup,
            "托盘重量": float(p_wt) if pd.notna(p_wt) else None,
            "托盘体积": float(p_vol) if p_vol is not None else None,
            "长(in)": round(float(L_in), 2) if pd.notna(L_in) else None,
            "宽(in)": round(float(W_in), 2) if pd.notna(W_in) else None,
            "高(in)": round(float(H_in), 2) if pd.notna(H_in) else None,
            "托盘创建日期": create_date_str,
            "托盘创建时间": create_time_str,
            "运单数量": len(waybills),
            "运单清单": ", ".join(waybills_disp) if waybills_disp else "",
            "运单箱数": ", ".join(waybills_disp_qty) if waybills_disp_qty else "",
            "自提仓库(按运单)": ", ".join(pickup_list_disp) if pickup_list_disp else "",
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

@st.cache_data(ttl=30)
def load_shipped_pallet_ids(_bust=0, sheet_sig=None) -> set[str]:
    try:
        ws = client.open(SHEET_SHIP_TRACKING).sheet1
    except SpreadsheetNotFound:
        return set()
    vals = _safe_get_all_values(ws, "UNFORMATTED_VALUE", "SERIAL_NUMBER")
    if not vals or not vals[0]:
        return set()
    header = list(vals[0])
    if "托盘号" not in header:
        return set()
    col_idx = header.index("托盘号")
    pallet_ids = [row[col_idx] for row in vals[1:] if len(row) > col_idx]
    def _norm_pid(s):
        return str(s).strip().upper() if s and str(s).strip() else ""
    return { _norm_pid(pid) for pid in pallet_ids if _norm_pid(pid) }

@st.cache_data(ttl=300)
def load_bol_waybill_costs(_bust=0) -> pd.DataFrame:
    try:
        ws = client.open(SHEET_BOL_DETAIL).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame(columns=["运单号","客户单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"])
    vals = _safe_get_all_values(ws, "UNFORMATTED_VALUE", "SERIAL_NUMBER")
    if not vals or not vals[0]:
        return pd.DataFrame(columns=["运单号","客户单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"])
    raw_header = list(vals[0])
    df = pd.DataFrame(vals[1:], columns=raw_header) if len(vals) > 1 else pd.DataFrame(columns=raw_header)
    def norm(s: str) -> str:
        return str(s).replace("\u00A0"," ").replace("\n","").replace(" ","").strip().lower()
    aliases = {
        "wb":   ["运单号","Waybill","waybill","运单编号","提单号","单号"],
        "cust": ["客户单号","客户PO","客户PO号","客户参考号","CustomerPO","CustomerRef","Reference","Ref","参考号"],
        "truck":["到自提仓库卡车号","到自提仓卡车号","到自提仓车号","BOL卡车号","BOL卡车","卡车单号","卡车号","TruckNo","Truck","truckno","truck"],
        "cost": ["到自提仓库费用","到自提仓费用","自提费用","BOL费用","Amount","amount","Cost","cost","费用","分摊费用"],
        "date": ["到自提仓库日期","到自提仓日期","自提日期","BOL日期","日期","Date","date","ETA(到自提仓)","ETA到自提仓库","到自提仓库ETA"],
    }
    def pick_col(cands: list[str]) -> str | None:
        wants = {norm(x) for x in cands}
        for h in raw_header:
            if norm(h) in wants:
                return h
        return None
    col_wb    = pick_col(aliases["wb"])
    col_cust  = pick_col(aliases["cust"])
    col_truck = pick_col(aliases["truck"])
    col_cost  = pick_col(aliases["cost"])
    col_date  = pick_col(aliases["date"])
    if not col_wb:
        return pd.DataFrame(columns=["运单号","客户单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"])
    need_cols = [c for c in [col_wb, col_cust, col_truck, col_cost, col_date] if c]
    df = df[need_cols].copy()
    rename_map = {}
    if col_wb:    rename_map[col_wb]    = "运单号"
    if col_cust:  rename_map[col_cust]  = "客户单号"
    if col_truck: rename_map[col_truck] = "到自提仓库卡车号"
    if col_cost:  rename_map[col_cost]  = "到自提仓库费用"
    if col_date:  rename_map[col_date]  = "到自提仓库日期"
    df.rename(columns=rename_map, inplace=True)

    if "运单号" in df.columns:
        df["运单号"] = df["运单号"].map(_norm_waybill_str)
    if "客户单号" in df.columns:
        df["客户单号"] = df["客户单号"].astype(str).str.strip()
    if "到自提仓库卡车号" in df.columns:
        df["到自提仓库卡车号"] = df["到自提仓库卡车号"].astype(str).str.strip()

    if "到自提仓库费用" in df.columns:
        def _to_num_safe_local(x):
            try:
                s = str(x).strip().replace(",", "")
                s = re.sub(r"[^\d\.\-]", "", s)
                return float(s)
            except Exception:
                return None
        df["到自提仓库费用"] = df["到自提仓库费用"].map(_to_num_safe_local)

    if "到自提仓库日期" in df.columns:
        df["_date_tmp"] = df["到自提仓库日期"].map(_parse_sheet_value_to_date)
        df["到自提仓库日期"] = df["_date_tmp"].map(lambda d: d.strftime("%Y-%m-%d") if isinstance(d, date) else pd.NA)
        df.drop(columns=["_date_tmp"], inplace=True, errors="ignore")

    df = df[df["运单号"].astype(str).str.strip() != ""]
    if not df.empty:
        df = df.drop_duplicates(subset=["运单号"], keep="last")

    for c in ["客户单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"]:
        if c not in df.columns:
            df[c] = pd.NA
    df["到自提仓库费用"] = pd.to_numeric(df["到自提仓库费用"], errors="coerce").round(2)
    return df[["运单号","客户单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"]]

@st.cache_data(ttl=30)
def load_ship_tracking_raw(_bust=0, sheet_sig=None) -> pd.DataFrame:
    try:
        ws = client.open(SHEET_SHIP_TRACKING).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame(columns=["托盘号","运单清单","卡车单号","分摊费用","日期","自提仓库(按托盘)"])
    vals = _safe_get_all_values(ws, "UNFORMATTED_VALUE", "SERIAL_NUMBER")
    if not vals or not vals[0]:
        return pd.DataFrame(columns=["托盘号","运单清单","卡车单号","分摊费用","日期","自提仓库(按托盘)"])
    raw_header = list(vals[0])
    df = pd.DataFrame(vals[1:], columns=raw_header) if len(vals) > 1 else pd.DataFrame(columns=raw_header)
    def norm(s: str) -> str:
        return str(s).replace("\u00A0"," ").replace("\n","").replace(" ","").strip().lower()
    aliases = {
        "托盘号": ["托盘号","托盘编号","PalletID","Pallet","托盘"],
        "运单清单": ["运单清单","Waybills","WaybillList","运单号","Waybill"],
        "卡车单号": ["卡车单号","TruckNo","Truck","卡车号"],
        "分摊费用": ["分摊费用","费用","Cost","Amount"],
        "日期":   ["日期","Date","发货日期","UploadDate"],
        "自提仓库(按托盘)": ["自提仓库(按托盘)","自提仓库","仓库","PickupWarehouse"]
    }
    def pick_col(cands: list[str]) -> str | None:
        wants = {norm(x) for x in cands}
        for h in raw_header:
            if norm(h) in wants:
                return h
        return None
    rename_map = {}
    for key, cands in aliases.items():
        c = pick_col(cands)
        if c:
            rename_map[c] = key
    df = df.rename(columns=rename_map)
    for c in ["托盘号","运单清单","卡车单号","分摊费用","日期","自提仓库(按托盘)"]:
        if c not in df.columns:
            df[c] = pd.NA
    df["托盘号"]   = df["托盘号"].astype(str).str.strip()
    df["卡车单号"] = df["卡车单号"].astype(str).str.strip()
    df["自提仓库(按托盘)"] = df["自提仓库(按托盘)"].astype(str).str.strip()
    df["分摊费用"] = pd.to_numeric(df["分摊费用"], errors="coerce").round(2)
    df["_day_obj"] = df["日期"].apply(_parse_sheet_value_to_date)
    df["日期"] = df["_day_obj"].apply(lambda d: d.strftime("%Y-%m-%d") if isinstance(d, date) else "")
    df.drop(columns=["_day_obj"], inplace=True, errors="ignore")
    df = df[df["托盘号"].astype(str).str.strip() != ""]
    return df[["托盘号","运单清单","卡车单号","分摊费用","日期","自提仓库(按托盘)"]]

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

# ===================== 运单增量构建（修改点：只识别 USSH，客户单号仅来自『到仓数据表』） =====================
def build_waybill_delta(track_override: pd.DataFrame | None = None):
    arrivals = load_arrivals_df(_bust=_get_bust("arrivals"))
    bol      = load_bol_waybill_costs(_bust=_get_bust("bol_detail"))

    ship_track_sig = _sheet_row_sig(SHEET_SHIP_TRACKING, _bust=_get_bust("ship_tracking"))
    track = load_ship_tracking_raw(
        _bust=_get_bust("ship_tracking"),
        sheet_sig=ship_track_sig
    )

    if track_override is None:
        track_override = st.session_state.get("_track_override", None)
    if isinstance(track_override, pd.DataFrame) and not track_override.empty:
        need_cols = ["托盘号","运单清单","卡车单号","分摊费用","日期"]
        track_override = track_override[[c for c in need_cols if c in track_override.columns]].copy()
        track = pd.concat([track, track_override], ignore_index=True)

    if track is None or track.empty:
        return pd.DataFrame(columns=[
            "运单号","客户单号","仓库代码","收费重","体积",
            "发出(ETD/ATD)","到港(ETA/ATA)",
            "到自提仓库日期","发走日期","到仓日期",
            "到自提仓库卡车号","到自提仓库费用",
            "发走卡车号","发走费用","自提仓库"
        ])

    def _norm_pid(s):
        return str(s).strip().upper() if pd.notna(s) else ""
    def _norm_trk(s):
        s = str(s).strip().upper() if pd.notna(s) else ""
        return s.replace(" ", "").replace("-", "")
    def _norm_day(s):
        dt = _parse_sheet_value_to_date(s)
        return _fmt_date(dt) if dt else ""

    track = track.copy()
    track["_pid_k"] = track.get("托盘号","").map(_norm_pid)
    track["_trk_k"] = track.get("卡车单号","").map(_norm_trk)
    track["_day_k"] = track.get("日期","").map(_norm_day)

    track = track[~track.duplicated(subset=["_pid_k","_trk_k","_day_k"], keep="last")].copy()
    track["_has_day"] = track["_day_k"].ne("")
    track = (
        track.sort_values(["_pid_k","_trk_k","_has_day"], ascending=[True, True, False])
             .drop_duplicates(subset=["_pid_k","_trk_k"], keep="first")
             .drop(columns=["_has_day"])
    )
    if "分摊费用" in track.columns:
        track["_cost2"] = pd.to_numeric(track["分摊费用"], errors="coerce").round(2)
        dup2 = track["_day_k"].eq("") & track.duplicated(subset=["_pid_k","_trk_k","_cost2"], keep="last")
        track = track[~dup2].copy()

    # ✅ 仅用严格 USSH 提取
    wb_lists = []
    for _, r in track.iterrows():
        wb_list = _extract_wb_ushh_only(r.get("运单清单",""))
        wb_lists.append(wb_list)
    track["_wb_list"] = wb_lists

    wb_from_track = set()
    for lst in track["_wb_list"]:
        if lst:
            wb_from_track.update(lst)

    if not wb_from_track:
        return pd.DataFrame(columns=[
            "运单号","客户单号","仓库代码","收费重","体积",
            "发出(ETD/ATD)","到港(ETA/ATA)",
            "到自提仓库日期","发走日期","到仓日期",
            "到自提仓库卡车号","到自提仓库费用",
            "发走卡车号","发走费用","自提仓库"
        ])

    if not arrivals.empty:
        arrivals = arrivals[arrivals["运单号"].isin(wb_from_track)].copy()
    if not bol.empty:
        bol = bol[bol["运单号"].isin(wb_from_track)].copy()

    weight_map = dict(zip(
        arrivals["运单号"],
        pd.to_numeric(arrivals["收费重"], errors="coerce")
    ))

    pickup_map = load_bol_pickup_map(_bust=_get_bust("bol_detail"))

    wb2_cost: dict[str, float] = {}
    wb2_trucks: dict[str, set] = {}
    wb2_date: dict[str, date] = {}

    wb_weight_cache: dict[str, float | None] = {}
    def _wb_weight(wb: str):
        if wb not in wb_weight_cache:
            wb_weight_cache[wb] = weight_map.get(wb, None)
        return wb_weight_cache[wb]

    for _, r in track.iterrows():
        waybills = [wb for wb in (r.get("_wb_list") or []) if wb in wb_from_track]
        if not waybills:
            continue
        pallet_cost = _to_num_safe(r.get("分摊费用"))
        truck_no    = r.get("卡车单号", "")
        dt_str      = r.get("日期", None)
        dt_obj      = _parse_sheet_value_to_date(dt_str) if not _is_blank(dt_str) else None

        total_w = 0.0
        weights = []
        for wb in waybills:
            w = _wb_weight(wb)
            if w and w > 0:
                weights.append(w); total_w += w
            else:
                weights.append(None)

        if total_w > 0:
            shares = [(w/total_w if (w and w > 0) else 0.0) for w in weights]
        else:
            shares = [1.0/len(waybills)] * len(waybills)

        if pallet_cost is not None:
            for wb, s in zip(waybills, shares):
                wb2_cost[wb] = wb2_cost.get(wb, 0.0) + pallet_cost * s

        if str(truck_no).strip():
            for wb in waybills:
                wb2_trucks.setdefault(wb, set()).add(str(truck_no).strip())

        if dt_obj:
            for wb in waybills:
                if (wb not in wb2_date) or (dt_obj < wb2_date[wb]):
                    wb2_date[wb] = dt_obj

    total_from_track = pd.to_numeric(track.get("分摊费用"), errors="coerce").fillna(0).sum()
    total_to_waybill = sum(wb2_cost.values())
    diff_total = round(total_from_track - total_to_waybill, 2)
    st.write(f"🧮 费用检查：发货追踪合计={total_from_track:.2f}，已分到运单={total_to_waybill:.2f}，差额={diff_total:.2f}")
    if "_trk_k" in track.columns:
        by_truck = (track
            .assign(_cost = pd.to_numeric(track["分摊费用"], errors="coerce").fillna(0))
            .groupby("_trk_k")["_cost"].sum())
        st.write("各卡车在『发货追踪』里的合计：", by_truck.to_dict())

    out = pd.DataFrame({"运单号": sorted(wb_from_track)})
    out["自提仓库"] = out["运单号"].map(lambda wb: pickup_map.get(wb, pd.NA))

    # ✅ 到仓数据（含 客户单号）对齐（客户单号只来自这里）
    if not arrivals.empty:
        arr2 = arrivals[["运单号","仓库代码","收费重","体积","ETD/ATD","ETA/ATA","预计到仓时间（日）","客户单号"]].copy()
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
        out["客户单号"]       = pd.NA

    if not bol.empty:
        out = out.merge(bol[["运单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"]], on="运单号", how="left")
    else:
        for c in ["到自提仓库日期","到自提仓库卡车号","到自提仓库费用"]:
            out[c] = pd.NA

    out["发走费用"]   = out["运单号"].map(lambda wb: round(wb2_cost.get(wb, 0.0), 2) if wb in wb2_cost else pd.NA)
    out["发走卡车号"] = out["运单号"].map(lambda wb: ", ".join(sorted(wb2_trucks.get(wb, []))) if wb in wb2_trucks else pd.NA)
    def _safe_fmt_date(d):
        return _fmt_date(d) if isinstance(d, date) else pd.NA
    out["发走日期"] = out["运单号"].map(lambda wb: _safe_fmt_date(wb2_date.get(wb)))

    out["收费重"]        = pd.to_numeric(out["收费重"], errors="coerce")
    out["体积"]          = pd.to_numeric(out["体积"], errors="coerce").round(2)
    out["到自提仓库费用"]  = pd.to_numeric(out["到自提仓库费用"], errors="coerce").round(2)
    out["发走费用"]       = pd.to_numeric(out["发走费用"], errors="coerce").round(2)
    out["美仓备货完成日期"] = out["到自提仓库日期"]

    final_cols = [
        "运单号","客户单号","仓库代码","自提仓库","收费重","体积",
        "发出(ETD/ATD)","到港(ETA/ATA)","美仓备货完成日期",
        "到自提仓库日期","发走日期","到仓日期",
        "到自提仓库卡车号","到自提仓库费用",
        "发走卡车号","发走费用"
    ]
    for c in final_cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out[final_cols]

MANAGED_COLS = [
    "运单号","客户单号",
    "发出(ETD/ATD)","到港(ETA/ATA)","美仓备货完成日期",
    "到自提仓库日期","发走日期","到仓日期",
    "到自提仓库卡车号","到自提仓库费用",
    "发走卡车号","发走费用",
    "仓库代码","自提仓库","收费重","体积",
    "批次ID","上传时间"
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

def upsert_waybill_summary_partial(df_delta: pd.DataFrame) -> bool:
    gc = get_gspread_client()

    # —— 打开『运单全链路汇总』——
    try:
        sh = gc.open(SHEET_WB_SUMMARY)
    except SpreadsheetNotFound:
        st.error(f"找不到工作表「{SHEET_WB_SUMMARY}」。")
        return False
    except Exception as e:
        st.error(f"打开『{SHEET_WB_SUMMARY}』失败：{e}")
        return False

    ws = sh.sheet1

    # —— 读现有数据 ——
    try:
        vals = _safe_get_all_values(ws)
    except Exception as e:
        st.error(f"读取『{SHEET_WB_SUMMARY}』失败：{e}")
        return False

    if not vals:
        st.error("『运单全链路汇总』为空，无法更新（至少需要表头）。")
        return False

    headers = _norm_header(vals[0])
    rows = vals[1:]
    exist_df = pd.DataFrame(rows, columns=headers)

    # ========= 标准化表头 =========
    df_delta = df_delta.copy()
    df_delta.columns = _norm_header(df_delta.columns)
    exist_df.columns = _norm_header(exist_df.columns)

    # ========= 补关键列 =========
    if "运单号" not in df_delta.columns:
        st.error("df_delta 缺少『运单号』列")
        return False
    if "运单号" not in exist_df.columns:
        st.error("『运单全链路汇总』缺少『运单号』列")
        return False

    # ========= 运单号标准化 =========
    df_delta["运单号"] = df_delta["运单号"].map(_norm_waybill_str)
    exist_df["运单号"] = exist_df["运单号"].map(_norm_waybill_str)

    # ========= 关键唯一性检查 =========
    try:
        _assert_unique_key(
            df_delta[df_delta["运单号"].ne("")],
            "运单号",
            df_name="本次运单增量(df_delta)"
        )
        _assert_unique_key(
            exist_df[exist_df["运单号"].ne("")],
            "运单号",
            df_name="运单全链路汇总"
        )
    except ValueError as e:
        st.error(str(e))
        return False

    # ========= 建行号 =========
    exist_df["_rowno"] = np.arange(2, 2 + len(exist_df))

    # ========= 建 index =========
    idx_delta = df_delta.set_index("运单号", drop=False)
    idx_exist = exist_df.set_index("运单号", drop=False)

    # ========= 需要同步的列 =========
    update_cols = [
        c for c in [
            "客户单号",
            "仓库代码",
            "收费重",
            "体积",
            "发出(ETD/ATD)",
            "到港(ETA/ATA)",
            "到自提仓库日期",
            "发走日期",
            "到仓日期",
            "到自提仓库卡车号",
            "到自提仓库费用",
            "发走卡车号",
            "发走费用",
            "自提仓库",
        ]
        if c in df_delta.columns
    ]

    # ========= 现有表缺列时补空列 =========
    new_cols_added = False
    for c in update_cols:
        if c not in exist_df.columns:
            exist_df[c] = ""
            headers.append(c)
            new_cols_added = True

    # 重新对齐列顺序（如有新增列）
    exist_df = exist_df.reindex(
        columns=[c for c in headers if c in exist_df.columns]
                + [c for c in exist_df.columns if c not in headers]
    )

    # 重新建立 index，保证新增列后仍可取值
    idx_exist = exist_df.set_index("运单号", drop=False)

    # ========= 新增 / 更新拆分 =========
    delta_wb = [wb for wb in df_delta["运单号"].tolist() if not _is_blank(wb)]
    exist_wb_set = set([wb for wb in exist_df["运单号"].tolist() if not _is_blank(wb)])

    to_update = [wb for wb in delta_wb if wb in exist_wb_set]
    to_append = [wb for wb in delta_wb if wb not in exist_wb_set]

    # ========= 构造更新 payload =========
    rows_payload_by_col = {}

    for col in update_cols:
        if col not in exist_df.columns:
            continue

        col_idx_1based = exist_df.columns.get_loc(col) + 1
        rowvals = []

        for wb in to_update:
            try:
                new_v = _scalar_loc(idx_delta, wb, col, df_name="本次运单增量(df_delta)")
                old_v = _scalar_loc(idx_exist, wb, col, df_name="运单全链路汇总")
                rno   = int(_scalar_loc(idx_exist, wb, "_rowno", df_name="运单全链路汇总"))
            except ValueError as e:
                st.error(str(e))
                return False

            new_j = _to_jsonable_cell(new_v)
            old_j = _to_jsonable_cell(old_v)

            # 仅当新值有效且与旧值不同才更新
            if _is_effective(new_j) and str(new_j) != str(old_j):
                rowvals.append((rno, [new_j]))

        if rowvals:
            rows_payload_by_col[col_idx_1based] = _pack_ranges_for_col(
                ws.title, col_idx_1based, rowvals
            )

    # ========= 先补表头新增列（如果有） =========
    if new_cols_added:
        try:
            ws.update("1:1", [list(exist_df.columns)], value_input_option="RAW")
        except Exception as e:
            st.error(f"更新表头失败：{e}")
            return False

    # ========= 批量更新已有行 =========
    batch_updates = []
    for _, updates in rows_payload_by_col.items():
        batch_updates.extend(updates)

    if batch_updates:
        try:
            ws.batch_update(batch_updates, value_input_option="USER_ENTERED")
        except Exception as e:
            st.error(f"批量更新已有行失败：{e}")
            return False

    # ========= 追加新行 =========
    if to_append:
        append_rows = []
        for wb in to_append:
            row = []
            for col in exist_df.columns:
                if col == "_rowno":
                    continue
                if col in idx_delta.columns:
                    try:
                        v = _scalar_loc(idx_delta, wb, col, df_name="本次运单增量(df_delta)")
                    except ValueError as e:
                        st.error(str(e))
                        return False
                    row.append(_to_jsonable_cell(v))
                else:
                    row.append("")
            append_rows.append(row)

        if append_rows:
            try:
                ws.append_rows(append_rows, value_input_option="USER_ENTERED")
            except Exception as e:
                st.error(f"追加新行失败：{e}")
                return False

    return True

# ========= UI =========
st.title("🚚 发货调度")

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
    c1,_ = st.columns([1,6])
    with c1:
        if st.button("🔄 刷新数据", key="btn_refresh_all"):
            for k in ["pallet_detail", "ship_tracking", "arrivals", "bol_detail", "wb_summary"]:
                _bust(k)
            for k in ["sel_locked", "locked_df", "_last_upload_pallets", "_last_upload_truck", "_last_upload_at", "all_snapshot_df", "_track_override"]:
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()

    arrivals_df = load_arrivals_df(_bust=_get_bust("arrivals"))
    bol_df      = load_bol_waybill_costs(_bust=_get_bust("bol_detail"))
    pallet_df   = load_pallet_detail_df(
        arrivals_df=arrivals_df,
        bol_cost_df=bol_df,
        _bust=_get_bust("pallet_detail"),
        refresh_token=(
            _get_bust("pallet_detail")
            + _get_bust("ship_tracking")
            + _get_bust("arrivals")
            + _get_bust("bol_detail")
        )
    )

    if pallet_df.empty:
        st.warning("未从『托盘明细表』读取到数据，请检查表名/权限/表头。")
        st.stop()

    ship_track_sig = _sheet_row_sig(SHEET_SHIP_TRACKING, _bust=_get_bust("ship_tracking"))
    shipped_pallets_raw = load_shipped_pallet_ids(
        _bust=_get_bust("ship_tracking"),
        sheet_sig=ship_track_sig
    )
    shipped_pallets_norm = {str(x).strip().upper() for x in shipped_pallets_raw}

    pallet_df["托盘号_norm"] = pallet_df["托盘号"].astype(str).str.strip().str.upper()
    pallet_df = pallet_df[~pallet_df["托盘号_norm"].isin(shipped_pallets_norm)]

    if pallet_df.empty:
        st.info("当前可发货的托盘为空（可能都已记录在『发货追踪』）。")
        st.stop()

    pk_opts = ["（全部）"] + sorted([x for x in pallet_df["自提仓库(按托盘)"].dropna().astype(str).unique() if x.strip()])
    pickup_pick = st.selectbox("选择自提仓库（可选）", options=pk_opts, key="pickup_pallet")
    if pickup_pick != "（全部）":
        pallet_df = pallet_df[pallet_df["自提仓库(按托盘)"] == pickup_pick]   

    wh_opts = ["（全部）"] + sorted([w for w in pallet_df["仓库代码"].dropna().unique() if str(w).strip()])
    wh_pick = st.selectbox("选择仓库代码（可选）", options=wh_opts, key="wh_pallet")
    if wh_pick != "（全部）":
        pallet_df = pallet_df[pallet_df["仓库代码"]==wh_pick]

    show_cols = [
        "托盘号","仓库代码","自提仓库(按托盘)","托盘重量","长(in)","宽(in)","高(in)","托盘体积",
        "托盘创建日期","托盘创建时间",
        "运单数量","运单清单","运单箱数","自提仓库(按运单)",
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
            with st.expander("展开查看未锁定托盘（点击展开/折叠）", expanded=False):
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
        cc1, cc2, cc3 = st.columns([2,2,2])
        with cc1:
            pallet_truck_no = st.text_input("卡车单号（必填）", key="pallet_truck_no")
        with cc2:
            pallet_total_cost = st.number_input("本车总费用（必填）", min_value=0.0, step=1.0, format="%.2f", key="pallet_total_cost")
        with cc3:
            ship_date_input = st.date_input("发货日期（默认今天）", value=date.today(), key="pallet_ship_date")

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
            idx = selected_pal["分摊费用"].idxmax()
            selected_pal.loc[idx, "分摊费用"] = round(
                selected_pal.loc[idx, "分摊费用"] + diff_cost, 2
            )

        upload_df = selected_pal.copy()
        upload_df["卡车单号"] = pallet_truck_no
        upload_df["总费用"] = round(float(pallet_total_cost), 2)
        upload_df["分摊比例"] = (upload_df["分摊比例"]*100).round(2).astype(str) + "%"
        upload_df["分摊费用"] = upload_df["分摊费用"].map(lambda x: f"{x:.2f}")
        upload_df["总费用"] = upload_df["总费用"].map(lambda x: f"{x:.2f}")
        upload_df["托盘体积"] = pd.to_numeric(upload_df.get("托盘体积", pd.Series()), errors="coerce").round(2)
        upload_df["上传发货日期（预览）"] = ship_date_input.strftime("%Y-%m-%d")

        preview_cols_pal = [
            "卡车单号","上传发货日期（预览）","仓库代码","托盘号","托盘重量","长(in)","宽(in)","高(in)","托盘体积",
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
        **分摊比例** = 托盘重量 ÷ 所选托盘总重量  
        **分摊费用** = 分摊比例 × 本车总费用（自动用最大项吸收几分钱差额）
        """)

        if st.button("📤 上传到『发货追踪』", key="btn_upload_pallet_upload_only"):
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
            _ship_date_str = ship_date_input.strftime("%Y-%m-%d")

            _date_header_candidates = ["日期", "发货日期", "出仓日期", "Date", "ShipDate"]
            date_col_to_use = None
            for cand in _date_header_candidates:
                if cand in header_raw:
                    date_col_to_use = cand
                    break
            if date_col_to_use is not None:
                tmp[date_col_to_use] = _ship_date_str

            _pickup_header_candidates = ["自提仓库", "自提仓", "Pickup", "pickup"]
            pickup_col_to_use = None
            for cand in _pickup_header_candidates:
                if cand in header_raw:
                    pickup_col_to_use = cand
                    break
            if pickup_col_to_use is not None:
                tmp[pickup_col_to_use] = upload_df.get("自提仓库(按托盘)", "").fillna("")

            def _norm_hdr(s: str) -> str:
                return str(s).replace("\u00A0"," ").replace("\n","").replace(" ","").strip().lower()
            _pid_candidates = ["托盘号","托盘编号","托盘id","托盘#",
                               "PalletID","PalletNo","palletid","palletno","pallet编号","pallet#","pallet"]
            cand_norm_set = {_norm_hdr(x) for x in _pid_candidates}

            pid_col_to_use = None
            for h in header_raw:
                if _norm_hdr(h) in cand_norm_set:
                    pid_col_to_use = h
                    break
            if pid_col_to_use is None:
                st.error("『发货追踪』缺少“托盘号”列（或等价列）。请先在目标表增加该列。")
                st.stop()

            tmp[pid_col_to_use] = upload_df["托盘号"].astype(str).str.strip()

            for col in header_raw:
                if col not in tmp.columns:
                    tmp[col] = ""
            rows = tmp.reindex(columns=header_raw).fillna("").values.tolist()

            ws_track.append_rows(rows, value_input_option="USER_ENTERED")
            st.success(f"✅ 已上传 {len(rows)} 条到『{SHEET_SHIP_TRACKING}』。卡车单号：{pallet_truck_no}")

            _bust("ship_tracking")
            _ = load_ship_tracking_raw(_bust=_get_bust("ship_tracking"))

            st.session_state["_last_upload_pallets"] = set(upload_df["托盘号"].astype(str).str.strip())
            st.session_state["_last_upload_truck"] = str(pallet_truck_no).strip()
            st.session_state["_last_upload_at"] = datetime.now()

            override = upload_df[[
                "托盘号","运单清单","自提仓库(按托盘)","分摊费用","上传发货日期（预览）","卡车单号"
            ]].copy()
            override = override.rename(columns={"上传发货日期（预览）": "日期"})
            def _to_float_safe(v):
                try:
                    return float(str(v).strip())
                except Exception:
                    return None
            override["托盘号"]   = override["托盘号"].astype(str).str.strip()
            override["卡车单号"] = override["卡车单号"].astype(str).str.strip()
            override["分摊费用"] = override["分摊费用"].map(_to_float_safe)
            override["日期"] = pd.to_datetime(override["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
            override["自提仓库(按托盘)"] = override["自提仓库(按托盘)"].astype(str).str.strip()
            st.session_state["_track_override"] = override

            st.info("下一步：点击下方“🔁 更新到『运单全链路汇总』”。")

        disable_b = not bool(st.session_state.get("_last_upload_pallets"))
        if st.button("🔁 更新到『运单全链路汇总』", key="btn_update_wb_summary", disabled=disable_b):
            needed_pids = st.session_state.get("_last_upload_pallets", set())

            def _wait_visibility(max_wait_s=6.0, poll_every=0.6) -> bool:
                start = time.time()
                while True:
                    track_now = load_ship_tracking_raw(_bust=_get_bust("ship_tracking"))
                    if not track_now.empty:
                        seen_pids = set(track_now.get("托盘号","").astype(str).str.strip())
                        if needed_pids & seen_pids:
                            return True
                    if time.time() - start > max_wait_s:
                        return False
                    time.sleep(poll_every)

            visible = _wait_visibility()
            if not visible:
                st.info("提示：远端可能存在短暂一致性延迟，已继续尝试同步…")

            try:
                df_delta = build_waybill_delta(track_override=st.session_state.get("_track_override"))
            except Exception as e:
                st.error(f"构建增量失败：{e}")
                st.stop()

            if df_delta.empty:
                time.sleep(1.2)
                _bust("ship_tracking")
                _ = load_ship_tracking_raw(_bust=_get_bust("ship_tracking"))
                try:
                    df_delta = build_waybill_delta()
                except Exception as e:
                    st.error(f"二次构建增量失败：{e}")
                    st.stop()

            if df_delta.empty:
                st.warning("没有可更新的运单：可能仍在远端延迟，或本次上传未包含可解析的运单号。稍后再试或刷新缓存。")
            else:
                try:
                    ok = upsert_waybill_summary_partial(df_delta)
                    if ok:
                        if "_track_override" in st.session_state:
                            del st.session_state["_track_override"]
                        st.success("✅ 已更新到『运单全链路汇总』")
                except Exception as e:
                    st.error(f"写入『运单全链路汇总』失败：{e}")
                    st.stop()

                if ok:
                    st.session_state["_wb_updated_at"] = time.time()
                    _bust("wb_summary")
                    _ = load_waybill_summary_df(_bust=_get_bust("wb_summary"))
                    st.success(f"✅ 已更新/新增 {len(df_delta)} 条到『{SHEET_WB_SUMMARY}』。")
                    st.rerun()
                else:
                    st.warning("未能写入『运单全链路汇总』：请检查表头（需包含“运单号”）或权限。")

with tab2:
    if st.session_state.get("_wb_updated_at"):
        if time.time() - float(st.session_state["_wb_updated_at"]) < 30:
            _bust("wb_summary")
            _ = load_waybill_summary_df(_bust=_get_bust("wb_summary"))
        del st.session_state["_wb_updated_at"]

    st.subheader("🚚 按卡车回填到仓日期")

    df_sum, ws_sum, header_raw = load_waybill_summary_df(_bust=_get_bust("wb_summary"))

    if ws_sum is None:
        st.info("未找到『运单全链路汇总』表。请先创建该表（至少包含表头『运单号』）。")
    elif df_sum.empty:
        st.info("『运单全链路汇总』当前为空。请先在『按托盘发货』上传数据后，再回到此处回填。")
    else:
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
            filt &= df_sum["_到仓日期_dt"].index.isin(df_sum[df_sum["发走卡车号"].astype(str).isin(trucks_pick)].index)
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
        fill_date = st.date_input("填充到仓日期", value=today)

        def _write_arrival_date(rows_idx, date_to_fill: date):
            col_idx_1based = None
            for i, h in enumerate(header_raw):
                hn = str(h).replace(" ", "")
                if hn in ["到仓日期", "到仓日", "到仓(wh)"]:
                    col_idx_1based = i + 1
                    break
            if col_idx_1based is None:
                st.error("目标表缺少『到仓日期』列。请先在表头新增该列后重试。")
                return False
            if not rows_idx:
                return True
            try:
                rows = sorted(int(r) for r in rows_idx if r is not None)
            except Exception:
                st.error("行号列表格式异常。")
                return False
            if not rows:
                return True

            ranges = []
            s = p = rows[0]
            for r in rows[1:]:
                if r == p + 1:
                    p = r
                else:
                    ranges.append((s, p))
                    s = p = r
            ranges.append((s, p))

            sheet_title = ws_sum.title
            spreadsheet_id = ws_sum.spreadsheet.id
            date_str = date_to_fill.strftime("%Y-%m-%d")

            def _mk_update_for_segment(r1, r2):
                a1_start = gspread.utils.rowcol_to_a1(r1, col_idx_1based)
                a1_end   = gspread.utils.rowcol_to_a1(r2, col_idx_1based)
                num_rows = r2 - r1 + 1
                return {
                    "range": f"{sheet_title}!{a1_start}:{a1_end}",
                    "values": [[date_str] for _ in range(num_rows)]
                }

            updates = [_mk_update_for_segment(r1, r2) for (r1, r2) in ranges]

            try:
                batch_size = 500
                for i in range(0, len(updates), batch_size):
                    sub = updates[i:i + batch_size]
                    body = {"valueInputOption": "USER_ENTERED", "data": sub}
                    sheets_service.spreadsheets().values().batchUpdate(
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
            if st.button("📝 写入到仓日期", key="btn_fill_arrival_date"):
                if df_target.empty:
                    st.warning("筛选结果为空；请调整仓库/卡车/日期条件。")
                else:
                    ok = _write_arrival_date(df_target["_rowno"].tolist(), fill_date)
                    if ok:
                        st.success(f"已更新 {len(df_target)} 行的『到仓日期』为 {fill_date.strftime('%Y-%m-%d')}。")
                        _bust("wb_summary")
                        st.rerun()
