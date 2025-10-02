# =========================================================
# transfer_min_api_app.py —— “转仓追踪”极简录入（Sheets API 直连）
# 读：『运单全链路汇总』；写：『转仓追踪』
# 需求要点：
# - 搜索多选运单 → 统一填 3 个字段（转仓卡车单号/转仓发出成本/转仓目的地仓点）
# - 将来源整行复制到目标，并在【最后】追加 4 列：
#     转仓卡车单号 | 转仓发出成本 | 转仓目的地仓点 | 最后更新时间
# - 若已存在该运单：整行覆盖；否则追加
# - 完全用 Google Sheets API 读写，绕过 gspread 的 AuthorizedSession 兼容问题
# =========================================================

import streamlit as st
from streamlit.errors import StreamlitAPIException
try:
    if not st.session_state.get("_page_configured", False):
        st.set_page_config(page_title="转仓追踪", page_icon="🚚", layout="wide")
        st.session_state["_page_configured"] = True
except StreamlitAPIException:
    pass

import pandas as pd
import numpy as np
import re
from datetime import datetime

# ---- Google API ----
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========================= 配置 =========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
SHEET_WB_SUMMARY = "运单全链路汇总"   # 来源：整行复制
SHEET_TRANSFER   = "转仓追踪"               # 目标：覆盖/追加

COL_WAYBILL = "运单号"
COL_T_TRUCK = "转仓卡车单号"
COL_T_COST  = "转仓发出成本"
COL_T_DEST  = "转仓目的地仓点"
COL_UPDATED = "最后更新时间"
TRANSFER_COLS = [COL_T_TRUCK, COL_T_COST, COL_T_DEST, COL_UPDATED]

DEST_OPTIONS = ["HNB_TX_US","ARF_GA_US","RDW_CA_US","MPR_NJ_US","BLL_NJ_US"]

# ===================== 授权与客户端 =====================
@st.cache_resource
def get_clients():
    if "gcp_service_account" in st.secrets:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("service_accounts.json", scopes=SCOPES)
    # gspread 仅用于 open 拿 worksheet（不做 HTTP 读写）
    gc = gspread.authorize(creds)
    # 真正读写走 Sheets API
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return gc, svc

gc, sheets_service = get_clients()

# ===================== 通用工具 =====================
def _norm_wb(v):
    s = "" if v is None else str(v).strip()
    if s.endswith(".0"): s = s[:-2]
    try:
        f = float(s)
        if abs(f - round(f)) < 1e-9: s = str(int(round(f)))
    except: pass
    return s

def _is_blank(v):
    try:
        if v is None: return True
        if isinstance(v, str) and v.strip() == "": return True
        return pd.isna(v)
    except: return False

def _a1_sheet(title: str) -> str:
    t = str(title)
    if any(ch in t for ch in [" ", "!", "'", ",", ";", ":", "[", "]"]):
        return "'" + t.replace("'", "''") + "'"
    return t

def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def _sanitize_header(header: list[str]) -> list[str]:
    """清洗表头：去空白、去 None；确保唯一（重复加 _1/_2 后缀）"""
    base = [("" if h is None else str(h).strip()) for h in header]
    seen = {}
    out = []
    for h in base:
        key = h if h else "col"
        if key not in seen:
            seen[key] = 0
            out.append(key)
        else:
            seen[key] += 1
            out.append(f"{key}_{seen[key]}")
    return out

def _pad_rows_to_header(rows: list[list], n_cols: int) -> list[list]:
    """把每行补齐/截断至与表头同列数"""
    fixed = []
    for r in rows:
        r = list(r) if isinstance(r, (list, tuple)) else [r]
        if len(r) < n_cols:
            r = r + [""] * (n_cols - len(r))
        elif len(r) > n_cols:
            r = r[:n_cols]
        fixed.append(r)
    return fixed

# ================= Sheets API 直连读写 =================
def api_values_get_all(ws, major_dim="ROWS"):
    """Sheets API 读取整表 (header, rows)"""
    spreadsheet_id = ws.spreadsheet.id
    title = _a1_sheet(ws.title)
    a1 = f"{title}!A1:ZZZ100000"
    try:
        resp = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=a1,
            majorDimension=major_dim
        ).execute()
        values = resp.get("values", []) or []
        if not values:
            return [], []
        return values[0], values[1:]
    except HttpError as e:
        raise RuntimeError(f"Sheets API get 失败：{e}")

def api_values_update_row(ws, row_index_1based: int, row_values: list[str], n_cols: int):
    """Sheets API 覆盖更新某一整行 A..end"""
    spreadsheet_id = ws.spreadsheet.id
    title = _a1_sheet(ws.title)
    end_col = _col_letter(max(1, n_cols))
    a1 = f"{title}!A{row_index_1based}:{end_col}{row_index_1based}"
    body = {"values": [row_values]}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=a1,
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

def api_values_append_rows(ws, rows_matrix: list[list[str]]):
    """Sheets API 追加多行"""
    if not rows_matrix:
        return
    spreadsheet_id = ws.spreadsheet.id
    title = _a1_sheet(ws.title)
    a1 = f"{title}!A1"
    body = {"values": rows_matrix}
    sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=a1,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

def api_values_update_header(ws, header_row: list[str]):
    """Sheets API 覆盖第 1 行表头"""
    spreadsheet_id = ws.spreadsheet.id
    title = _a1_sheet(ws.title)
    a1 = f"{title}!1:1"
    body = {"values": [header_row]}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=a1,
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

# ===================== 读取来源表 =====================
@st.cache_data(ttl=30)
def load_full_chain(_bust=0):
    try:
        ws = gc.open(SHEET_WB_SUMMARY).sheet1
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"找不到工作表「{SHEET_WB_SUMMARY}」")
        return pd.DataFrame(), None, []
    header, rows = api_values_get_all(ws)
    if not header:
        st.warning("『运单全链路汇总』为空。")
        return pd.DataFrame(), ws, []
    header = _sanitize_header(header)
    rows = _pad_rows_to_header(rows, len(header))
    df = pd.DataFrame(rows, columns=header) if rows else pd.DataFrame(columns=header)
    # 规范“运单号”
    if COL_WAYBILL not in df.columns:
        for c in ["Waybill","waybill","运单编号","单号"]:
            if c in df.columns:
                df = df.rename(columns={c: COL_WAYBILL})
                break
        else:
            df[COL_WAYBILL] = ""
            header = header + [COL_WAYBILL]
    df[COL_WAYBILL] = df[COL_WAYBILL].map(_norm_wb)
    return df, ws, header

# ===================== 读取/初始化目标表 =====================
@st.cache_data(ttl=30)
def load_transfer(_bust=0):
    try:
        ss = gc.open(SHEET_TRANSFER)
        ws = ss.sheet1
    except gspread.exceptions.SpreadsheetNotFound:
        ss = gc.create(SHEET_TRANSFER)
        ws = ss.sheet1
        # 初始化最小表头（稍后 upsert 会按规则扩展并把四列放到最后）
        api_values_update_header(ws, [COL_WAYBILL])

    header, rows = api_values_get_all(ws)
    if not header:
        api_values_update_header(ws, [COL_WAYBILL])
        header, rows = api_values_get_all(ws)

    header = _sanitize_header(header)
    rows = _pad_rows_to_header(rows, len(header))
    df = pd.DataFrame(rows, columns=header) if rows else pd.DataFrame(columns=header)

    if COL_WAYBILL not in df.columns:
        df[COL_WAYBILL] = ""
        header = header + [COL_WAYBILL]
        api_values_update_header(ws, header)

    df[COL_WAYBILL] = df[COL_WAYBILL].map(_norm_wb)
    if not df.empty:
        df["_rowno"] = np.arange(2, 2 + len(df))
    else:
        df["_rowno"] = pd.Series(dtype=int)
    return df, ws, header

# ===================== Upsert（四列固定在最后） =====================
def upsert_transfer_rows(ws, header_now: list[str], rows_dicts: list[dict]):
    # 1) 当前表头/数据
    cur_header, cur_rows = api_values_get_all(ws)
    if not cur_header:
        cur_header = [COL_WAYBILL]
        api_values_update_header(ws, cur_header)
        cur_rows = []

    cur_header = _sanitize_header(cur_header)

    # 2) 目标表头 = 现有表头去掉四列 + rows_dicts 出现的新列（也去掉四列） + 四列固定在最后
    def strip_transfer(cols_iter):
        return [c for c in cols_iter if c not in TRANSFER_COLS]

    seen = set()
    keys_all = []

    # 2.1 保持当前非转仓列的顺序
    for h in strip_transfer(cur_header):
        if h not in seen:
            seen.add(h); keys_all.append(h)

    # 2.2 rows_dicts 中的其它列（非转仓列）也要补进来，避免丢字段
    for rd in rows_dicts:
        for k in strip_transfer(rd.keys()):
            if k not in seen:
                seen.add(k); keys_all.append(k)

    # 2.3 四个转仓列追加到最末尾（保证存在且在最后）
    keys_all += TRANSFER_COLS

    # 如果表头发生变化，则更新表头，并立刻把现有行按新表头长度 pad
    if keys_all != cur_header:
        api_values_update_header(ws, keys_all)
        cur_header = keys_all
        cur_rows = _pad_rows_to_header(cur_rows, len(cur_header))

    # 3) 建当前索引（运单 -> 行号）—— 先按当前表头长度 pad，再建 DF
    fixed_rows = _pad_rows_to_header(cur_rows, len(cur_header)) if cur_rows else []
    df_cur = pd.DataFrame(fixed_rows, columns=cur_header) if fixed_rows else pd.DataFrame(columns=cur_header)
    if COL_WAYBILL not in df_cur.columns:
        df_cur[COL_WAYBILL] = ""
    df_cur[COL_WAYBILL] = df_cur[COL_WAYBILL].astype(str).str.strip()
    wb2row = {}
    if not df_cur.empty:
        df_cur["_rowno"] = np.arange(2, 2 + len(df_cur))
        for wb, rno in zip(df_cur[COL_WAYBILL], df_cur["_rowno"]):
            if wb:
                wb2row[wb] = int(rno)

    # 4) 组装“更新行/追加行”
    updates = []  # (rowno, row_values)
    appends = []  # [row_values]

    for rd in rows_dicts:
        wb = str(rd.get(COL_WAYBILL, "")).strip()
        # 保证四列键存在
        for c in TRANSFER_COLS:
            rd.setdefault(c, "")

        # 按当前表头顺序取值
        row_vals = []
        for h in cur_header:
            v = rd.get(h, "")
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                v = ""
            row_vals.append(v)

        if wb in wb2row:
            updates.append((wb2row[wb], row_vals))
        else:
            appends.append(row_vals)

    # 5) 写入：逐行 update + 批量 append
    for (rno, row_vals) in updates:
        api_values_update_row(ws, rno, row_vals, n_cols=len(cur_header))
    if appends:
        api_values_append_rows(ws, appends)

    return True

# =========================== UI ===========================
st.title("🚚 转仓追踪")

c1, _ = st.columns([1,6])
with c1:
    if st.button("🔄 刷新数据", use_container_width=True):
        st.session_state["_bust"] = int(st.session_state.get("_bust", 0)) + 1
        st.rerun()

# 读取两张表
df_full, ws_full, hdr_full = load_full_chain(_bust=st.session_state.get("_bust", 0))
df_trn,  ws_trn,  hdr_trn  = load_transfer(_bust=st.session_state.get("_bust", 0))

if df_full.empty:
    st.stop()

# 1) 搜索多选
st.subheader("1) 选择运单")
q = st.text_input("搜索运单号")
mask = pd.Series(True, index=df_full.index)
if q.strip():
    keys = [k.strip() for k in re.split(r"\s+", q.strip()) if k.strip()]
    for k in keys:
        mask &= df_full[COL_WAYBILL].astype(str).str.contains(re.escape(k), case=False, na=False)

wb_list = df_full.loc[mask, COL_WAYBILL].dropna().astype(str).tolist()
sel = st.multiselect("匹配到的运单：", options=wb_list, default=[], placeholder="选择要录入转仓的运单…")

# 2) 统一输入三字段
st.subheader("2) 输入统一的转仓信息")
t_truck = st.text_input(COL_T_TRUCK, placeholder="例如：TRK-2025-0001")
t_cost  = st.number_input(COL_T_COST, min_value=0.0, format="%.2f", step=1.0)
t_dest  = st.selectbox(COL_T_DEST, options=DEST_OPTIONS, index=0)

st.divider()

# 3) 写入
if st.button("📤 写入『转仓追踪』", type="primary"):
    if not sel:
        st.warning("请先选择至少一个运单。")
        st.stop()
    if _is_blank(t_truck) or _is_blank(t_dest):
        st.warning("请填写完整：转仓卡车单号、目的地仓点。")
        st.stop()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    idx_full = df_full.set_index(COL_WAYBILL, drop=False)

    rows_to_write = []
    for wb in sel:
        wb_norm = _norm_wb(wb)
        if wb_norm not in idx_full.index:
            st.warning(f"运单 {wb_norm} 不在『运单全链路汇总』中，已跳过。")
            continue
        src_row = idx_full.loc[wb_norm]
        # 以当前来源 DataFrame 的列（已对齐、消毒）复制整行
        rd = {h: src_row.get(h, "") for h in df_full.columns}
        # 附加四列（放到最后由 upsert 决定表头顺序）
        rd[COL_WAYBILL] = wb_norm
        rd[COL_T_TRUCK] = t_truck
        rd[COL_T_COST]  = float(t_cost)
        rd[COL_T_DEST]  = t_dest
        rd[COL_UPDATED] = now_str
        rows_to_write.append(rd)

    if not rows_to_write:
        st.warning("没有可写入的记录。")
        st.stop()

    ok = upsert_transfer_rows(ws_trn, hdr_trn, rows_to_write)
    if ok:
        st.success(f"✅ 已写入『转仓追踪』：{len(rows_to_write)} 条。")
        st.session_state["_bust"] = int(st.session_state.get("_bust", 0)) + 1
        st.rerun()
