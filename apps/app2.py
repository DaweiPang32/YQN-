# 功能：
# - 托盘重量/体积：重量只来自《托盘明细表》并按托盘求和；体积由长宽高（inch）计算为 CBM（每个托盘只计算一次，避免重复）
# - ETA/ATA（合并列）、ETD/ATD（Excel序列 45824 等）→ 日期字符串
# - 对客承诺送仓时间如“19-21”→ 与今天的天数差：x-y（锚定 ETA/ATA 的月份，缺失用当月）
# - 已发托盘读取自『发货追踪』，再次进入页面自动隐藏
# - 上传到『发货追踪』后，自动【部分更新】『运单全链路汇总』
#   仅更新以下列：客户单号、发出(ETD/ATD)、到港(ETA/ATA)、到自提仓库日期、到自提仓库卡车号、到自提仓库费用、发走日期、发走卡车号、发走费用
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
        # 已有其它页面/模块设置过，忽略重复设置
        pass
    st.session_state["_page_configured"] = True
# ==============================================

# ========= 预编译正则 =========
_RE_PARENS = re.compile(r"[\(\（][\s\S]*?[\)\）]", re.DOTALL)
_RE_SPLIT = re.compile(r"[,\，;\；、\|\/\s]+")
_RE_NUM = re.compile(r'[-+]?\d+(?:\.\d+)?')

def _split_tokens(s: str) -> list[str]:
    """快速分词：按通用分隔符切分并剔除空白"""
    if not isinstance(s, str):
        s = str(s) if s is not None else ""
    return [t for t in _RE_SPLIT.split(s) if t]
def _remove_parens_iter(s: str) -> str:
    """反复去掉半角/全角括号内的内容（支持嵌套），直到不能再去。"""
    if not isinstance(s, str) or not s:
        return ""
    prev = None
    out = s
    while prev != out:
        prev = out
        # 先半角，再全角
        out = re.sub(r"\([^()]*\)", "", out)
        out = re.sub(r"（[^（）]*）", "", out)
    return out

def _first_balanced_paren_content(s: str) -> str | None:
    """
    返回字符串 s 中【第一个成对括号】内的完整内容（支持嵌套、支持全角/半角）。
    优先匹配半角()；若未找到再尝试全角（）。
    """
    if not isinstance(s, str) or not s:
        return None

    # 半角
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

    # 全角
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
    """
    返回 (gspread_client, sheets_service)
    - gspread_client: 兼容你现有的所有 ws 读取/写入
    - sheets_service: 官方 Sheets v4 Service，用于 batchUpdate 等高效写
    """
    if "gcp_service_account" in st.secrets:
        sa_info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("service_accounts.json", scopes=SCOPES)

    gc = gspread.authorize(creds)
    # cache_discovery=False 可避免不必要的网络请求
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return gc, svc

def get_gspread_client():
    # 兼容旧调用：内部改为复用统一 Clients
    gc, _ = get_clients()
    return gc

client, sheets_service = get_clients()
# ========= 表名配置 =========
SHEET_ARRIVALS_NAME   = "到仓数据表"       # ETD/ATD、ETA/ATA（合并）、对客承诺送仓时间、预计到仓时间（日）
SHEET_PALLET_DETAIL   = "托盘明细表"       # 托盘数据（重量/体积来自此表；体积由 L/W/H(inch) 计算为 CBM）
SHEET_SHIP_TRACKING   = "发货追踪"         # 托盘维度出仓记录（分摊到托盘）
SHEET_BOL_DETAIL      = "bol自提明细"      # 到自提仓库 明细（分摊到运单）
SHEET_WB_SUMMARY      = "运单全链路汇总"    # 仅部分更新

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

def _pack_ranges_for_col(ws_title: str, col_idx_1based: int, rowvals: list[tuple[int, list]]):
    """
    将同一列的 (行号, [单元格值]) 列表压成若干个连续 A1 区段，返回 Sheets batchUpdate 需要的 dict 列表。
    - ws_title: 工作表名（如 'Sheet1'）
    - col_idx_1based: 列号（1 起）
    - rowvals: [(row_index, [value]), ...]，必须已按 row_index 升序
    返回: [{"range": "Sheet1!B2:B10", "values": [[...],[...],...]} , ...]
    """
    updates = []
    if not rowvals:
        return updates

    # 合并连续行
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
    # 尾段
    a1s = gspread.utils.rowcol_to_a1(s, col_idx_1based)
    a1e = gspread.utils.rowcol_to_a1(p, col_idx_1based)
    updates.append({"range": f"{ws_title}!{a1s}:{a1e}", "values": buf})
    return updates

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
# ==== 轻量 HTTP 退避调用（用于 Sheets API 原生调用）====
def _with_backoff(fn, *args, **kwargs):
    """
    对 Google API 调用做最多 6 次指数退避（~0.3 + 0.6 + 1.2 + 2.4 + 4.8 + 6.0s）。
    用于 values.get / values.batchGet / spreadsheets.get 等。
    """
    delays = [0.3, 0.6, 1.2, 2.4, 4.8, 6.0]
    last_err = None
    for i, d in enumerate([0.0] + delays):
        if d > 0:
            time.sleep(d + random.random() * 0.2)
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            # 只对 429/配额 做重试，其它错误抛出
            msg = str(e)
            if ("429" in msg) or ("Quota" in msg) or ("quota" in msg):
                last_err = e
                continue
            raise
    # 到这里说明重试已用尽
    if last_err:
        raise last_err

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
        nums = _RE_NUM.findall(s)   # 使用预编译的数字正则
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
@st.cache_data(ttl=10)
def _sheet_row_sig(sheet_name: str, _bust=0) -> tuple[int, int]:
    """
    返回 (rows, cols) 作为工作表的“轻量签名”：
    - rows: 当前表的总行数
    - cols: 当前表中最长一行的列数
    作用：参与下游缓存 key，确保【改表头/新增列】也会触发相关缓存失效。
    """
    try:
        ws = client.open(sheet_name).sheet1
    except SpreadsheetNotFound:
        return (0, 0)

    vals = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
    if not vals:
        return (0, 0)

    rows = len(vals)
    # 计算当前表中“最长一行”的列数，捕获只改列数时的变化
    cols = max((len(r) for r in vals), default=0)
    return (rows, cols)


@st.cache_data(ttl=300)
def load_bol_pickup_map(_bust=0) -> dict:
    """从『bol自提明细test』构建 运单号→自提仓库 的映射。"""
    try:
        ws = client.open(SHEET_BOL_DETAIL).sheet1
    except SpreadsheetNotFound:
        return {}
    vals = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
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

    mapping = dict(zip(df[col_wb], df[col_pk]))
    return mapping

@st.cache_data(ttl=30)
def load_arrivals_df(_bust=0) -> pd.DataFrame:
    """
    读取『到仓数据表』，只保留下游用到的列，并统一 dtype。
    返回列（全部可用）：
      运单号、仓库代码、收费重、体积、
      ETA/ATA、ETD/ATD、对客承诺送仓时间、预计到仓时间（日）、
      _ETAATA_date（date对象转成字符串前的锚点，用于送仓时段差值计算）
    """
    try:
        ws = client.open(SHEET_ARRIVALS_NAME).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame(columns=[
            "运单号","仓库代码","收费重","体积",
            "ETA/ATA","ETD/ATD","对客承诺送仓时间","预计到仓时间（日）",
            "_ETAATA_date"
        ])

    vals = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
    if not vals or not vals[0]:
        return pd.DataFrame(columns=[
            "运单号","仓库代码","收费重","体积",
            "ETA/ATA","ETD/ATD","对客承诺送仓时间","预计到仓时间（日）",
            "_ETAATA_date"
        ])

    header = _norm_header(vals[0])
    df = pd.DataFrame(vals[1:], columns=header)

    # 别名/存在性处理
    if "运单号" not in df.columns: df["运单号"] = pd.NA
    if "仓库代码" not in df.columns: df["仓库代码"] = pd.NA
    if "收费重" not in df.columns: df["收费重"] = pd.NA

    # 体积列自适配
    vol_col = next((c for c in ["体积","CBM","体积m3","体积(m3)","体积（m3）"] if c in df.columns), None)
    if vol_col is None:
        df["体积"] = pd.NA
    else:
        df["体积"] = pd.to_numeric(df[vol_col], errors="coerce")

    # ETA/ATA 来源（可能是“ETA/ATA”或合并列“ETAATA”）
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

    # 规范化
    df["运单号"] = df["运单号"].apply(_norm_waybill_str)
    df["仓库代码"] = df["仓库代码"].astype(str).str.strip()
    df["收费重"] = pd.to_numeric(df["收费重"], errors="coerce")

    # 解析日期：ETA/ATA 锚点 + 格式化
    if etaata_src is not None:
        df["_ETAATA_date"] = df[etaata_src].apply(_parse_sheet_value_to_date)
        df["ETA/ATA"] = df["_ETAATA_date"].apply(_fmt_date).replace("", pd.NA)
    else:
        df["_ETAATA_date"] = pd.NA
        df["ETA/ATA"] = pd.NA

    # ETD/ATD 格式化
    df["_ETD_ATD_date"] = df["ETD/ATD"].apply(_parse_sheet_value_to_date)
    df["ETD/ATD"] = df["_ETD_ATD_date"].apply(_fmt_date).replace("", pd.NA)

    # 预计到仓时间（日）格式化
    df["_ETA_WH_date"] = df[eta_wh_col].apply(_parse_sheet_value_to_date)
    df["预计到仓时间（日）"] = df["_ETA_WH_date"].apply(_fmt_date).replace("", pd.NA)

    # 去重（同运单保留最后一条）
    df = df.drop_duplicates(subset=["运单号"], keep="last")

    keep = ["仓库代码","运单号","收费重","体积",
            "ETA/ATA","ETD/ATD","对客承诺送仓时间","预计到仓时间（日）",
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

# ====== 补丁D: 增加 refresh_token 参与 cache key，确保刷新后聚合重算 ======
@st.cache_data(ttl=300)
def load_pallet_detail_df(arrivals_df: pd.DataFrame | None = None,
                          bol_cost_df: pd.DataFrame | None = None,
                          _bust=0,
                          refresh_token: int = 0) -> pd.DataFrame:
    """
    托盘维度：从《托盘明细表》聚合，并与《到仓数据表》匹配时间/承诺字段；
    同时引入『bol自提明细test』中的“自提仓库”映射，生成：
      - 自提仓库(按托盘)：若托盘内所有运单对应同一自提仓库则取该值；否则为“（多自提仓）”；若都无映射则空
      - 自提仓库(按运单)：逐单展示，形如 "WB1(A仓), WB2(B仓), ..."
    返回托盘粒度 DataFrame。
    注：refresh_token 仅参与缓存 key，无需在函数体内使用。
    """
    # === 读托盘明细 ===
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

    # --- 关键列统一 ---
    # 托盘号
    if "托盘号" not in df.columns:
        for cand in ["托盘ID","托盘编号","PalletID","PalletNo","palletid","palletno"]:
            if cand in df.columns:
                df = df.rename(columns={cand: "托盘号"})
                break
    if "托盘号" not in df.columns:
        df["托盘号"] = pd.NA

    # 仓库代码
    if "仓库代码" not in df.columns:
        df["仓库代码"] = pd.NA

    # 运单号
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

    # 重量列
    weight_col = None
    for cand in ["托盘重量","托盘重","收费重","托盘收费重","计费重","计费重量","重量"]:
        if cand in df.columns:
            weight_col = cand
            break
    if weight_col is None:
        df["托盘重量"] = pd.NA
        weight_col = "托盘重量"
    df[weight_col] = pd.to_numeric(df[weight_col], errors="coerce")

    # 长宽高（inch）
    len_col = next((c for c in ["托盘长","长","长度","Length","length","L"] if c in df.columns), None)
    wid_col = next((c for c in ["托盘宽","宽","宽度","Width","width","W"] if c in df.columns), None)
    hei_col = next((c for c in ["托盘高","高","高度","Height","height","H"] if c in df.columns), None)

    # 箱数
    qty_col = next((c for c in [
        "箱数","箱","件数","箱件数","Packages","Package","Cartons","Carton",
        "Qty","QTY","数量"
    ] if c in df.columns), None)
    if qty_col is None:
        df["箱数"] = pd.NA
        qty_col = "箱数"
    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce")

    # 体积计算（按行，使用 inch → m³）
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

    # 聚合函数
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

    # 创建时间列（来自 recv_app.py 提交）
    create_date_col = next((c for c in ["托盘创建日期","创建日期","PalletCreateDate","CreateDate"] if c in df.columns), None)
    create_time_col = next((c for c in ["托盘创建时间","创建时间","PalletCreateTime","CreateTime"] if c in df.columns), None)
    if create_date_col is None:
        df["托盘创建日期"] = ""
        create_date_col = "托盘创建日期"
    if create_time_col is None:
        df["托盘创建时间"] = ""
        create_time_col = "托盘创建时间"

    # === 先按 托盘号+仓库聚合（重量/体积/长宽高/运单清单/创建时间）===
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

    # === 依赖表：到仓数据表（ETA/ATA 等）、BOL成本（客户单号优先来源）===
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

    # === 自提仓库映射（运单→自提仓库）===
    pickup_map = load_bol_pickup_map(_bust=_get_bust("bol_detail"))

    # === 逐托盘构建输出行 ===
    pallets = []
    for _, brow in base.iterrows():
        pid, wh = brow["托盘号"], brow["仓库代码"]
        if _is_blank(pid):
            continue

        p_wt  = brow.get("托盘重量", None)
        p_vol = brow.get("托盘体积", None)

        waybills = brow.get("运单清单_list", []) or []
        # 展示：运单 + 客户单号
        waybills_disp = []
        for wb in waybills:
            wb_norm = _norm_waybill_str(wb)
            cust = cust_map.get(wb_norm, "")
            waybills_disp.append(f"{wb}({cust})" if cust else f"{wb}")

        # 托盘创建日期/时间
        create_date_str, create_time_str = _split_dt_to_date_time_str(
            brow.get("托盘创建日期_raw", ""),
            brow.get("托盘创建时间_raw", "")
        )

        # 每个运单的箱数合计（同托盘内）
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

        # 关联 ETA/ATA、ETD/ATD、承诺 & diff
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

        # 尺寸（inch）
        L_in = brow.get("托盘长in", None)
        W_in = brow.get("托盘宽in", None)
        H_in = brow.get("托盘高in", None)

        # === 自提仓库聚合 ===
        pickup_list = []
        pickup_list_disp = []  # 展示：WB(自提仓库)
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
            "托盘体积": float(p_vol) if p_vol is not None else None,  # m³
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

    # 只保留有托盘号的
    out = out[out["托盘号"].astype(str).str.strip() != ""].copy()

    # 数值列清洗 & 四舍五入
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
    """
    读取『发货追踪』，返回已发托盘号集合（标准化大写去空格）。
    """
    try:
        ws = client.open(SHEET_SHIP_TRACKING).sheet1
    except SpreadsheetNotFound:
        return set()

    vals = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
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
    """
    从『bol自提明细』读取并统一输出：
      运单号 / 客户单号 / 到自提仓库日期 / 到自提仓库卡车号 / 到自提仓库费用
    - 只保留必要列
    - 运单号规范化
    - 费用 -> float(两位)
    - 日期 -> 'YYYY-MM-DD'
    """
    try:
        ws = client.open(SHEET_BOL_DETAIL).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame(columns=["运单号","客户单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"])

    vals = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
    if not vals or not vals[0]:
        return pd.DataFrame(columns=["运单号","客户单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"])

    raw_header = list(vals[0])
    df = pd.DataFrame(vals[1:], columns=raw_header) if len(vals) > 1 else pd.DataFrame(columns=raw_header)

    # 别名映射
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

    # 只取需要的列（存在的部分）
    need_cols = [c for c in [col_wb, col_cust, col_truck, col_cost, col_date] if c]
    df = df[need_cols].copy()

    # 统一列名
    rename_map = {}
    if col_wb:    rename_map[col_wb]    = "运单号"
    if col_cust:  rename_map[col_cust]  = "客户单号"
    if col_truck: rename_map[col_truck] = "到自提仓库卡车号"
    if col_cost:  rename_map[col_cost]  = "到自提仓库费用"
    if col_date:  rename_map[col_date]  = "到自提仓库日期"
    df.rename(columns=rename_map, inplace=True)

    # 规范化
    if "运单号" in df.columns:
        df["运单号"] = df["运单号"].map(_norm_waybill_str)

    if "客户单号" in df.columns:
        df["客户单号"] = df["客户单号"].astype(str).str.strip()

    if "到自提仓库卡车号" in df.columns:
        df["到自提仓库卡车号"] = df["到自提仓库卡车号"].astype(str).str.strip()

    # 费用 -> float(两位)
    if "到自提仓库费用" in df.columns:
        def _to_num_safe_local(x):
            try:
                s = str(x).strip().replace(",", "")
                s = re.sub(r"[^\d\.\-]", "", s)
                return float(s)
            except Exception:
                return None
        df["到自提仓库费用"] = df["到自提仓库费用"].map(_to_num_safe_local)

    # 日期 -> YYYY-MM-DD
    if "到自提仓库日期" in df.columns:
        df["_date_tmp"] = df["到自提仓库日期"].map(_parse_sheet_value_to_date)
        df["到自提仓库日期"] = df["_date_tmp"].map(lambda d: d.strftime("%Y-%m-%d") if isinstance(d, date) else pd.NA)
        df.drop(columns=["_date_tmp"], inplace=True, errors="ignore")

    # 清理空运单 & 去重（保留最后一次）
    df = df[df["运单号"].astype(str).str.strip() != ""]
    if not df.empty:
        df = df.drop_duplicates(subset=["运单号"], keep="last")

    # 保证列齐全 + 类型
    for c in ["客户单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"]:
        if c not in df.columns:
            df[c] = pd.NA
    df["到自提仓库费用"] = pd.to_numeric(df["到自提仓库费用"], errors="coerce").round(2)

    return df[["运单号","客户单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"]]


@st.cache_data(ttl=30)
def load_ship_tracking_raw(_bust=0, sheet_sig=None) -> pd.DataFrame:
    """
    读取『发货追踪』，只保留必要列并统一列名：
      托盘号 / 运单清单 / 卡车单号 / 分摊费用 / 日期 / 自提仓库(按托盘)
    """
    try:
        ws = client.open(SHEET_SHIP_TRACKING).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame(columns=["托盘号","运单清单","卡车单号","分摊费用","日期","自提仓库(按托盘)"])

    vals = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
    if not vals or not vals[0]:
        return pd.DataFrame(columns=["托盘号","运单清单","卡车单号","分摊费用","日期","自提仓库(按托盘)"])

    raw_header = list(vals[0])
    df = pd.DataFrame(vals[1:], columns=raw_header) if len(vals) > 1 else pd.DataFrame(columns=raw_header)

    # 别名映射：兼容不同表头
    aliases = {
        "托盘号": ["托盘号","托盘编号","PalletID","Pallet","托盘"],
        "运单清单": ["运单清单","Waybills","WaybillList","运单号","Waybill"],
        "卡车单号": ["卡车单号","TruckNo","Truck","卡车号"],
        "分摊费用": ["分摊费用","费用","Cost","Amount"],
        "日期":   ["日期","Date","发货日期","UploadDate"],
        "自提仓库(按托盘)": ["自提仓库(按托盘)","自提仓库","仓库","PickupWarehouse"]
    }

    def norm(s: str) -> str:
        return str(s).replace("\u00A0"," ").replace("\n","").replace(" ","").strip().lower()

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

    # 保证列齐全
    for c in ["托盘号","运单清单","卡车单号","分摊费用","日期","自提仓库(按托盘)"]:
        if c not in df.columns:
            df[c] = pd.NA

    # 类型清洗
    df["托盘号"]   = df["托盘号"].astype(str).str.strip()
    df["卡车单号"] = df["卡车单号"].astype(str).str.strip()
    df["自提仓库(按托盘)"] = df["自提仓库(按托盘)"].astype(str).str.strip()

    # 分摊费用 -> float
    df["分摊费用"] = pd.to_numeric(df["分摊费用"], errors="coerce").round(2)

    # 日期 -> YYYY-MM-DD
    df["_day_obj"] = df["日期"].apply(_parse_sheet_value_to_date)
    df["日期"] = df["_day_obj"].apply(lambda d: d.strftime("%Y-%m-%d") if isinstance(d, date) else "")
    df.drop(columns=["_day_obj"], inplace=True, errors="ignore")

    # 清理空托盘
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

# ===================== 运单增量构建 =====================
def _extract_pure_waybills_and_po(mixed: str):
    """
    输入整段“运单清单”cell，返回 (wb_list, cust_map_from_cell)
      - wb_list: 解析出的运单号列表（括号去掉后再判定，避免跨段）
      - cust_map_from_cell: {wb: 第一个括号的原样内容}
        * 注意：不判断是不是 PO；只要在该段的第一个括号里，就原样写
    """
    wb_list = []
    cust_map = {}
    if _is_blank(mixed):
        return wb_list, cust_map

    # 按中文/英文逗号、分号、顿号、竖线、斜杠等切分为“每个片段”
    segs = re.split(r"[,\，;\；、\|/]+", str(mixed))
    for seg in segs:
        seg = seg.strip()
        if not seg:
            continue

        # 1) 取第一个括号的完整内容（支持嵌套；半角优先、再全角）
        first_paren_text = _first_balanced_paren_content(seg)

        # 2) 为了找 WB：把括号内容迭代删除，再做 token 判定
        seg_no_paren = _remove_parens_iter(seg)
        parts = _split_tokens(seg_no_paren)

        found_wb_for_this_seg = None
        for p in parts:
            token = _norm_waybill_str(p)
            if not token:
                continue
            # 排除以 IP 开头
            if token.upper().startswith("IP"):
                continue
            # 必须字母+数字且长度>=8
            if not (re.search(r"[A-Za-z]", token) and re.search(r"\d", token) and len(token) >= 8):
                continue
            wb_list.append(token)
            # 把“该段的第一个 WB”和“该段的第一个括号文本”关联起来
            if first_paren_text and (found_wb_for_this_seg is None):
                cust_map[token] = first_paren_text
                found_wb_for_this_seg = token

    # 去重保序
    seen = set(); out = []
    for wb in wb_list:
        if wb not in seen:
            seen.add(wb); out.append(wb)
    return out, cust_map

def _extract_pure_waybills(mixed: str) -> list[str]:
    """
    从《发货追踪》的“运单清单”字段中提取纯运单号列表。
    规则保持不变：去括号内容；必须字母+数字组合且长度>=8；排除以 IP 开头的 token。
    """
    if _is_blank(mixed):
        return []
    s = str(mixed).strip()
    # 去掉括号（含中文全角）中的注释/客户单号等信息
    s = _RE_PARENS.sub("", s).strip()
    if not s:
        return []
    parts = _split_tokens(s)

    out = []
    for p in parts:
        token = _norm_waybill_str(p)
        if not token:
            continue
        # 你的原始排除规则：IP 开头不算运单号
        if token.upper().startswith("IP"):
            continue
        # 必须包含字母和数字，且长度>=8
        if not (re.search(r"[A-Za-z]", token) and re.search(r"\d", token) and len(token) >= 8):
            continue
        out.append(token)
    return out


def build_waybill_delta(track_override: pd.DataFrame | None = None):
    """
    聚合到“运单粒度”的增量数据。
    优化点（对外行为不变）：
      - 统一对 track（含 override）做强标准化 + 去重；
      - 每行仅解析一次运单清单为 _wb_list；
      - 先生成 wb_from_track，后续表按集合裁剪，减少数据量；
      - 引入 wb_weight_cache，计算分摊避免重复开销。
    """
    # === 依赖数据 ===
    arrivals = load_arrivals_df(_bust=_get_bust("arrivals"))
    bol      = load_bol_waybill_costs(_bust=_get_bust("bol_detail"))

    # ✅ 用『发货追踪』当前表签名参与缓存键，保证删/增行即时生效
    ship_track_sig = _sheet_row_sig(SHEET_SHIP_TRACKING, _bust=_get_bust("ship_tracking"))
    track = load_ship_tracking_raw(
        _bust=_get_bust("ship_tracking"),
        sheet_sig=ship_track_sig
    )

    # === 合并 override（刚上传但远端未必可见的行）===
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

    # === 强标准化 + 去重（托盘/卡车/日期） ===
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

    # 1) 先按 (托盘, 卡车, 日期) 去重
    track = track[~track.duplicated(subset=["_pid_k","_trk_k","_day_k"], keep="last")].copy()

    # ✅ 1.5) 新增：同一(托盘,卡车)如仍有两条（一个空日期、一个有日期），优先保留“有日期”的那条
    track["_has_day"] = track["_day_k"].ne("")
    track = (
        track.sort_values(["_pid_k","_trk_k","_has_day"], ascending=[True, True, False])
            .drop_duplicates(subset=["_pid_k","_trk_k"], keep="first")
            .drop(columns=["_has_day"])
    )

    # 2) 兜底：对“日期仍为空”的重复，再按 (托盘, 卡车, round(分摊费用,2)) 去重
    if "分摊费用" in track.columns:
        track["_cost2"] = pd.to_numeric(track["分摊费用"], errors="coerce").round(2)
        dup2 = track["_day_k"].eq("") & track.duplicated(subset=["_pid_k","_trk_k","_cost2"], keep="last")
        track = track[~dup2].copy()


    # === 仅解析一次“运单清单”为列表 ===
    # === 同时解析“运单清单列表”与“第一个括号→客户单号覆盖表” ===
    track = track.copy()
    wb_lists = []
    cust_override_map = {}  # {wb: 来自该段第一个括号的文本}

    for _, r in track.iterrows():
        wb_list, cell_map = _extract_pure_waybills_and_po(r.get("运单清单",""))
        wb_lists.append(wb_list)
        for k, v in (cell_map or {}).items():
            if k and v:
                cust_override_map[k] = v

    track["_wb_list"] = wb_lists



    # === 汇总出本次涉及的运单集合 ===
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

    # === 只保留与本次相关的 arrivals / bol ===
    if not arrivals.empty:
        arrivals = arrivals[arrivals["运单号"].isin(wb_from_track)].copy()
    if not bol.empty:
        bol = bol[bol["运单号"].isin(wb_from_track)].copy()

    # === 预备映射 ===
    weight_map = dict(zip(
        arrivals["运单号"],
        pd.to_numeric(arrivals["收费重"], errors="coerce")
    ))

    # 自提仓库（运单级）映射
    pickup_map = load_bol_pickup_map(_bust=_get_bust("bol_detail"))

    # === 分摊累积容器 ===
    wb2_cost: dict[str, float] = {}
    wb2_trucks: dict[str, set] = {}
    wb2_date: dict[str, date] = {}

    # —— weight 缓存（避免每次 dict 取值 & 重复 sum）——
    wb_weight_cache: dict[str, float | None] = {}
    def _wb_weight(wb: str):
        if wb not in wb_weight_cache:
            wb_weight_cache[wb] = weight_map.get(wb, None)
        return wb_weight_cache[wb]

    # === 遍历track行做分摊 ===
    for _, r in track.iterrows():
        waybills = [wb for wb in (r.get("_wb_list") or []) if wb in wb_from_track]
        if not waybills:
            continue

        pallet_cost = _to_num_safe(r.get("分摊费用"))
        truck_no    = r.get("卡车单号", "")
        dt_str      = r.get("日期", None)
        dt_obj      = _parse_sheet_value_to_date(dt_str) if not _is_blank(dt_str) else None

        # 计算分摊权重
        total_w = 0.0
        weights = []
        for wb in waybills:
            w = _wb_weight(wb)
            if w and w > 0:
                weights.append(w)
                total_w += w
            else:
                weights.append(None)

        if total_w > 0:
            shares = [(w/total_w if (w and w > 0) else 0.0) for w in weights]
        else:
            shares = [1.0/len(waybills)] * len(waybills)

        # 费用分摊
        if pallet_cost is not None:
            for wb, s in zip(waybills, shares):
                wb2_cost[wb] = wb2_cost.get(wb, 0.0) + pallet_cost * s

        # 卡车号集合
        if str(truck_no).strip():
            for wb in waybills:
                wb2_trucks.setdefault(wb, set()).add(str(truck_no).strip())

        # 发走最早日期
        if dt_obj:
            for wb in waybills:
                if (wb not in wb2_date) or (dt_obj < wb2_date[wb]):
                    wb2_date[wb] = dt_obj

    # === 输出骨架 ===
    out = pd.DataFrame({"运单号": sorted(wb_from_track)})

    # 自提仓库（运单级）
    out["自提仓库"] = out["运单号"].map(lambda wb: pickup_map.get(wb, pd.NA))

    # 到仓数据对齐
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

    # ==== 客户单号优先级：cell括号(0) > BOL(1) > 托盘(2) > 到仓(3) ====

    def _build_cust_priority_map(cust_override_map: dict,
                                bol_df: pd.DataFrame,
                                wb_from_track: set[str]) -> pd.DataFrame:
        frames = []

        # 0) 来自“运单清单 cell 第一个括号”的覆盖
        if cust_override_map:
            cust_from_cell = pd.DataFrame(
                [{"运单号": wb, "客户单号": po}
                for wb, po in cust_override_map.items()
                if po is not None and str(po).strip() != ""]
            )
            if not cust_from_cell.empty:
                cust_from_cell["_pri"] = 0
                frames.append(cust_from_cell)

        # 1) BOL
        if bol_df is not None and not bol_df.empty and "客户单号" in bol_df.columns:
            tmp = bol_df[["运单号","客户单号"]].copy()
            tmp["_pri"] = 1
            frames.append(tmp)

        # 2) 托盘
        pal = load_customer_refs_from_pallet(_bust=_get_bust("pallet_detail"))
        if pal is not None and not pal.empty:
            tmp = pal.copy()
            tmp["_pri"] = 2
            frames.append(tmp)

        # 3) 到仓
        arr = load_customer_refs_from_arrivals(_bust=_get_bust("arrivals"))
        if arr is not None and not arr.empty:
            tmp = arr.copy()
            tmp["_pri"] = 3
            frames.append(tmp)

        if not frames:
            # 返回空骨架，避免后面 KeyError
            return pd.DataFrame(columns=["运单号","客户单号","_pri"])

        cust_all = pd.concat(frames, ignore_index=True)

        # 统一 & 过滤
        cust_all["运单号"] = cust_all["运单号"].map(_norm_waybill_str)
        cust_all["客户单号"] = cust_all["客户单号"].astype(str).str.strip()
        cust_all = cust_all[
            cust_all["运单号"].isin(wb_from_track) & (cust_all["客户单号"] != "")
        ]

        # 兜底：万一上面哪里漏了 _pri
        if "_pri" not in cust_all.columns:
            cust_all["_pri"] = 99

        # 稳定排序（同运单按优先级最小保留）
        cust_all = (cust_all
                    .sort_values(["运单号","_pri"], kind="mergesort")
                    .drop_duplicates(subset=["运单号"], keep="first")
                    )[["运单号","客户单号"]]

        return cust_all

    # —— 在你原位置调用 —— 
    cust_all = _build_cust_priority_map(cust_override_map, bol, wb_from_track)
    if not cust_all.empty:
        out = out.merge(cust_all, on="运单号", how="left")
    else:
        out["客户单号"] = pd.NA


    # BOL 自提字段
    if not bol.empty:
        out = out.merge(bol[["运单号","到自提仓库日期","到自提仓库卡车号","到自提仓库费用"]], on="运单号", how="left")
    else:
        for c in ["到自提仓库日期","到自提仓库卡车号","到自提仓库费用"]:
            out[c] = pd.NA

    # 发走费用 / 发走卡车号 / 发走日期
    out["发走费用"]   = out["运单号"].map(lambda wb: round(wb2_cost.get(wb, 0.0), 2) if wb in wb2_cost else pd.NA)
    out["发走卡车号"] = out["运单号"].map(lambda wb: ", ".join(sorted(wb2_trucks.get(wb, []))) if wb in wb2_trucks else pd.NA)
    def _safe_fmt_date(d):
        return _fmt_date(d) if isinstance(d, date) else pd.NA

    out["发走日期"] = out["运单号"].map(lambda wb: _safe_fmt_date(wb2_date.get(wb)))


    # 数值清洗
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

def upsert_waybill_summary_partial(df_delta: pd.DataFrame):
    """
    只对 MANAGED_COLS 做“定点值更新”或“追加新行”，并加入『写入策略』保护人工修改：
      - blank_only：仅当目标单元格为空时才写
      - merge_set ：把新旧字符串按分隔符合并去重
      - default   ：有有效值就写、空值不写

    本版优化：
      - 使用 _pack_ranges_for_col 把同一列的行写入压成连续 A1 区段；
      - 用 sheets_service.spreadsheets().values().batchUpdate 分批提交
    """
    WRITE_POLICY = {
        "到仓日期": "blank_only",
        "发走日期": "blank_only",
        "美仓备货完成日期": "blank_only",
        "到自提仓库日期": "blank_only",
        "到自提仓库费用": "blank_only",
        "发走费用": "blank_only",
        "到自提仓库卡车号": "merge_set",
        "发走卡车号": "merge_set",
        "仓库代码": "blank_only",
        "客户单号": "blank_only",
        "自提仓库": "blank_only",
        "批次ID": "blank_only",
        "上传时间": "blank_only",
    }
    MERGE_SEP = ","

    def _cell_blank(x):
        return (x is None) or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == "")

    def _merge_set(old, new):
        def toks(s):
            if _cell_blank(s): return []
            parts = re.split(r"[,\，;\；\|/ ]+", str(s).strip())
            return [p for p in parts if p]
        seen = []
        for t in (toks(old) + toks(new)):
            if t not in seen:
                seen.append(t)
        return MERGE_SEP.join(seen)
    def _is_iso_date(s: str) -> bool:
        try:
            if not isinstance(s, str):
                return False
            pd.to_datetime(s, format="%Y-%m-%d", errors="raise")
            return True
        except Exception:
            return False

    # --- 打开目标表 ---
    try:
        ws = client.open(SHEET_WB_SUMMARY).sheet1
    except SpreadsheetNotFound:
        st.error(f"找不到工作表「{SHEET_WB_SUMMARY}」。请先创建并在第1行写入表头（至少包含：运单号）。")
        return False

    vals = _safe_get_all_values(
        ws,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER"
    )
    if not vals or not vals[0]:
        st.error("『运单全链路汇总』为空且无表头。请先在第一行写好表头（至少包含：运单号）。")
        return False

    header = list(vals[0])
    if "运单号" not in header:
        st.error("『运单全链路汇总』缺少“运单号”表头，无法更新。")
        return False

    # --- 入参清洗 ---
    df_delta = df_delta.copy()
    if "运单号" not in df_delta.columns:
        st.error("增量数据缺少“运单号”。")
        return False
    df_delta["运单号"] = df_delta["运单号"].map(_norm_waybill_str)

    # --- 补齐缺列（一次性在表头追加缺失的受管列） ---
    missing_cols = [c for c in MANAGED_COLS if c not in header]
    if missing_cols:
        ws.update(f"{ws.title}!1:1", [header + missing_cols], value_input_option="USER_ENTERED")
        header = header + missing_cols

    # --- 现有表数据索引 ---
    exist_df = pd.DataFrame(vals[1:], columns=header) if len(vals) > 1 else pd.DataFrame(columns=header)
    if "运单号" not in exist_df.columns:
        exist_df["运单号"] = ""
    exist_df["运单号"] = exist_df["运单号"].map(_norm_waybill_str)
    exist_df["_rowno"] = np.arange(2, 2 + len(exist_df))

    idx_exist = exist_df.set_index("运单号", drop=False)
    idx_delta = df_delta.set_index("运单号", drop=False)

    common  = idx_delta.index.intersection(idx_exist.index)
    new_ids = list(idx_delta.index.difference(idx_exist.index))

    # ========== 老数据“定点列更新”：构造 updates，按列压区段 ==========
    updates = []

    for col in MANAGED_COLS:
        if col == "运单号":
            continue
        if col not in header or col not in idx_delta.columns:
            continue

        col_idx = header.index(col) + 1
        rows_payload = []  # (row_no, [value])

        policy = WRITE_POLICY.get(col, "default")
        is_date_col = col in ["到仓日期","发走日期","美仓备货完成日期","到自提仓库日期"]

        for wb in common:
            new_v = idx_delta.loc[wb, col]

            # 日期列只允许合法 YYYY-MM-DD 字符串写入，避免 0/空 被表格格式化成 1970-01-01
            if is_date_col:
                if not _is_iso_date(str(new_v)):
                    continue
            else:
                if not _is_effective(new_v):
                    continue

            rno = int(idx_exist.loc[wb, "_rowno"])
            old_v = idx_exist.loc[wb, col] if col in idx_exist.columns else ""

            if policy == "blank_only":
                if not (old_v is None or (isinstance(old_v, float) and pd.isna(old_v)) or (isinstance(old_v, str) and old_v.strip() == "")):
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
        updates.extend(_pack_ranges_for_col(ws.title, col_idx, rows_payload))


    # === 批量写入（老数据更新，分批提交更稳） ===
    if updates:
        spreadsheet_id = ws.spreadsheet.id
        batch_sz = 300  # 每批 300 段通常比较稳
        for i in range(0, len(updates), batch_sz):
            sub = updates[i:i + batch_sz]
            body = {"valueInputOption": "USER_ENTERED", "data": sub}
            sheets_service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()

    # ========== 新运单“追加新行” ==========
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
                        # merge_set 策略对新行不需要合并，直接写入
                        row_dict[c] = _to_jsonable_cell(v)
            new_rows.append([row_dict.get(c, "") for c in header])

        if new_rows:
            ws.append_rows(new_rows, value_input_option="USER_ENTERED")

    return True


# ========= UI：仅启用“按托盘发货” + 「按卡车回填到仓日期」 =========
st.title("🚚 发货调度")

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
    # ======= 补丁C：全量刷新按钮（托盘/已发/到仓/BOL/全链路） =======
    c1,_ = st.columns([1,6])
    with c1:
        if st.button("🔄 刷新数据", key="btn_refresh_all"):
            for k in ["pallet_detail", "ship_tracking", "arrivals", "bol_detail", "wb_summary"]:
                _bust(k)
            for k in ["sel_locked", "locked_df", "_last_upload_pallets", "_last_upload_truck", "_last_upload_at", "all_snapshot_df", "_track_override"]:
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()


    # 可选：先读依赖表，再注入到托盘读取，减少重复读
    arrivals_df = load_arrivals_df(_bust=_get_bust("arrivals"))
    bol_df      = load_bol_waybill_costs(_bust=_get_bust("bol_detail"))
    # ======= 补丁D：给聚合传入 refresh_token，确保 bust 后重算 =======
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

    # 取得『发货追踪』当前行数签名（10秒内变动就会触发重算）
    ship_track_sig = _sheet_row_sig(SHEET_SHIP_TRACKING, _bust=_get_bust("ship_tracking"))

    # 把签名传入，确保删除/新增后，这里会重新读取
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

    # ✅ 自提仓库筛选
    pk_opts = ["（全部）"] + sorted([x for x in pallet_df["自提仓库(按托盘)"].dropna().astype(str).unique() if x.strip()])
    pickup_pick = st.selectbox("选择自提仓库（可选）", options=pk_opts, key="pickup_pallet")
    if pickup_pick != "（全部）":
        pallet_df = pallet_df[pallet_df["自提仓库(按托盘)"] == pickup_pick]   
    
    # 仓库筛选
    wh_opts = ["（全部）"] + sorted([w for w in pallet_df["仓库代码"].dropna().unique() if str(w).strip()])
    wh_pick = st.selectbox("选择仓库代码（可选）", options=wh_opts, key="wh_pallet")
    if wh_pick != "（全部）":
        pallet_df = pallet_df[pallet_df["仓库代码"]==wh_pick]

    # 表格与勾选
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
            idx = selected_pal["分摊费用"].idxmax()  # 找到分摊费用最大的一行
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
        **分摊比例计算公式：** 每个托盘的分摊比例 = 该托盘重量 ÷ 所有选中托盘重量总和  
        **分摊费用计算公式：** 每个托盘的分摊费用 = 分摊比例 × 本车总费用  
        （最后一托盘自动调整几分钱差额，确保总额=本车总费用）
        """)

        # === 按钮A：仅上传到『发货追踪』 ===
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
            # === 覆写缓存（本地直推）：把刚上传的“托盘→发货追踪”行，保存为读取端可用的临时数据 ===
            # === 覆写缓存（本地直推）：把刚上传的“托盘→发货追踪”行，保存为读取端可用的临时数据 ===
            override = upload_df[[
                "托盘号","运单清单","自提仓库(按托盘)","分摊费用","上传发货日期（预览）","卡车单号"
            ]].copy()

            # 统一列名
            override = override.rename(columns={
                "上传发货日期（预览）": "日期"
            })

            # 类型清洗（确保分摊费用为 float、日期为标准字符串）
            override["托盘号"]   = override["托盘号"].astype(str).str.strip()
            override["卡车单号"] = override["卡车单号"].astype(str).str.strip()

            def _to_float_safe(v):
                try:
                    return float(str(v).strip())
                except Exception:
                    return None

            override["分摊费用"] = override["分摊费用"].map(_to_float_safe)

            override["日期"] = pd.to_datetime(override["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
            override["自提仓库(按托盘)"] = override["自提仓库(按托盘)"].astype(str).str.strip()

            st.session_state["_track_override"] = override


            st.info("下一步：点击下方“🔁 更新到『运单全链路汇总』”。")

        # === 按钮B：从『发货追踪』更新/补写到『运单全链路汇总』 ===
        disable_b = not bool(st.session_state.get("_last_upload_pallets"))
        if st.button("🔁 更新到『运单全链路汇总』", key="btn_update_wb_summary", disabled=disable_b):
            needed_pids = st.session_state.get("_last_upload_pallets", set())

            # ① 可见性轮询
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

            # ② 构建增量并写入全链路
            # ② 构建增量并写入全链路 —— 优先用 override（本地直推）
            try:
                df_delta = build_waybill_delta(track_override=st.session_state.get("_track_override"))
            except Exception as e:
                st.error(f"构建增量失败：{e}")
                st.stop()


            # ③ 兜底重读一次
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
                        # ✅ 成功写入后清理本地缓存，避免下一次重复叠加
                        if "_track_override" in st.session_state:
                            del st.session_state["_track_override"]
                        st.success("✅ 已更新到『运单全链路汇总』")
                except Exception as e:
                    st.error(f"写入『运单全链路汇总』失败：{e}")
                    st.stop()


                if ok:
                    # ===== 补丁A：写入成功后立刻刷新并重跑，保证切到 Tab2 看到新数据 =====
                    st.session_state["_wb_updated_at"] = time.time()
                    _bust("wb_summary")
                    _ = load_waybill_summary_df(_bust=_get_bust("wb_summary"))
                    st.success(f"✅ 已更新/新增 {len(df_delta)} 条到『{SHEET_WB_SUMMARY}』。")
                    st.rerun()
                else:
                    st.warning("未能写入『运单全链路汇总』：请检查表头（需包含“运单号”）或权限。")


with tab2:
    # ===== 补丁B：Tab2 进入即自动拉新（30秒窗口内刚更新过全链路则强制刷新一次） =====
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

        def _get_google_credentials():
            if "gcp_service_account" in st.secrets:
                sa_info = st.secrets["gcp_service_account"]
                return Credentials.from_service_account_info(sa_info, scopes=SCOPES)
            else:
                return Credentials.from_service_account_file("service_accounts.json", scopes=SCOPES)

        def _write_arrival_date(rows_idx, date_to_fill: date):
            """
            将 rows_idx（1-based 的行号）对应的『到仓日期』列写入指定日期。
            优化点：
            - 复用全局 sheets_service（减少重复握手，降低429）
            - 合并连续行成区段 A1
            - 超长写入按每 500 行/批分批提交（更稳）
            依赖：外层已有 ws_sum、header_raw（就是 load_waybill_summary_df 返回的）
            """
            # 1) 找到『到仓日期』列的 1-based 列号
            col_idx_1based = None
            for i, h in enumerate(header_raw):
                hn = str(h).replace(" ", "")
                if hn in ["到仓日期", "到仓日", "到仓(wh)"]:
                    col_idx_1based = i + 1
                    break
            if col_idx_1based is None:
                st.error("目标表缺少『到仓日期』列。请先在表头新增该列后重试。")
                return False

            # 空集合直接返回
            if not rows_idx:
                return True

            # 2) 规范化并排序行号
            try:
                rows = sorted(int(r) for r in rows_idx if r is not None)
            except Exception:
                st.error("行号列表格式异常。")
                return False
            if not rows:
                return True

            # 3) 合并连续行段 -> [(start_row, end_row), ...]
            ranges = []
            s = p = rows[0]
            for r in rows[1:]:
                if r == p + 1:
                    p = r
                else:
                    ranges.append((s, p))
                    s = p = r
            ranges.append((s, p))

            # 4) 组装 batchUpdate payload（每段一个 range，values 为逐行同值）
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

            # 5) 分批发送（每 500 段/批通常很稳；若单段很长也没关系）
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
