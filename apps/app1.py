# ship_app.py  —— 发货调度（无“批次”维度）
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound
from datetime import datetime, timedelta

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def get_gspread_client():
    if "gcp_service_account" in st.secrets:
        sa_info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
        return gspread.authorize(creds)
    else:
        creds = Credentials.from_service_account_file("service_accounts.json", scopes=SCOPES)
        return gspread.authorize(creds)

client = get_gspread_client()

# ========= 表名配置 =========
SHEET_ARRIVALS_NAME = "到仓数据表"
SHEET_BOL_NAME = "BOL自提"
SHEET_SHIP_DETAIL = "bol自提明细"

# ========= 工具函数 =========
def excel_serial_to_date(val):
    try:
        f = float(val)
        return datetime(1899, 12, 30) + timedelta(days=f)
    except Exception:
        return pd.NaT

@st.cache_data(ttl=60)
def load_bol_df():
    ws = client.open(SHEET_BOL_NAME).sheet1
    data = ws.get_all_values(value_render_option="UNFORMATTED_VALUE", date_time_render_option="SERIAL_NUMBER")
    if not data:
        return pd.DataFrame()
    header = [h.replace("\u00A0", " ").replace("\n", "").strip() for h in data[0]]
    df = pd.DataFrame(data[1:], columns=header)

    # 必要列兜底（不改表结构）
    for need in ["运单号", "客户单号", "ETA", "自提仓库"]:
        if need not in df.columns:
            df[need] = pd.NA

    df["运单号"] = df["运单号"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["运单号"])

    parsed_serial = df["ETA"].apply(excel_serial_to_date)
    fallback = pd.to_datetime(df["ETA"], errors="coerce")
    df["ETA"] = parsed_serial.combine_first(fallback)

    return df[["运单号", "客户单号", "ETA", "自提仓库"]]

@st.cache_data(ttl=60)
def load_arrivals_df():
    ws = client.open(SHEET_ARRIVALS_NAME).sheet1
    data = ws.get_all_values()
    if not data:
        return pd.DataFrame()
    header = [h.replace("\u00A0", "").replace("\n", "").replace(" ", "") for h in data[0]]
    df = pd.DataFrame(data[1:], columns=header)

    for need in ["运单号", "仓库代码", "收费重"]:
        if need not in df.columns:
            df[need] = pd.NA

    df["运单号"] = df["运单号"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["运单号"])
    df["收费重"] = pd.to_numeric(df["收费重"], errors="coerce")
    return df[["仓库代码", "运单号", "收费重"]]

@st.cache_data(ttl=60)
def load_shipped_waybills():
    try:
        ss = client.open(SHEET_SHIP_DETAIL)
        ws = ss.sheet1
    except SpreadsheetNotFound:
        return set()
    vals = ws.get_all_values()
    if not vals:
        return set()
    header = vals[0]
    rows = vals[1:]
    if "运单号" not in header:
        return set()
    idx = header.index("运单号")
    out = set()
    for r in rows:
        if len(r) > idx:
            wb = str(r[idx]).strip()
            if wb:
                out.add(wb)
    return out

# ========= 页面设置 =========
st.set_page_config(page_title="发货调度平台", layout="wide")
st.title("🚚 发货调度")

# ========= 刷新缓存 =========
left, right = st.columns([1,6])
with left:
    if st.button("🔄 刷新数据缓存"):
        st.cache_data.clear()
        st.rerun()

# ========= 数据源（合并）=========
arrivals_df = load_arrivals_df()
bol_df = load_bol_df()
if arrivals_df.empty and bol_df.empty:
    st.warning("没有从 Google Sheets 读取到数据，请检查表名/权限。")
    st.stop()

merged = bol_df.merge(arrivals_df, on="运单号", how="left")

base_cols = ["仓库代码", "运单号", "客户单号", "ETA", "收费重", "自提仓库"]  # 加上自提仓库
for c in base_cols:
    if c not in merged.columns:
        merged[c] = pd.NA

# 已发过滤
already = load_shipped_waybills()
if already:
    merged = merged[~merged["运单号"].astype(str).isin(already)]

# ========= 日期筛选 =========
st.markdown("### 🔎 筛选")
merged["ETA"] = pd.to_datetime(merged["ETA"], errors="coerce")
valid = merged["ETA"].dropna()
if not valid.empty:
    min_d, max_d = valid.min().date(), valid.max().date()
    default_start = max(max_d - timedelta(days=14), min_d)
    start_date, end_date = st.date_input(
        "按 ETA 选择日期范围",
        value=(default_start, max_d),
        min_value=min_d, max_value=max_d
    )
    mask = merged["ETA"].between(pd.to_datetime(start_date), pd.to_datetime(end_date))
    filtered_base = merged[mask].copy()
else:
    st.info("未检测到可解析的 ETA；将展示全部。")
    filtered_base = merged.copy()

# ========= 仓库筛选 =========
wh_options = filtered_base["仓库代码"].dropna().unique().tolist()
warehouse = st.selectbox("选择仓库代码（可选）", options=["（全部）"] + wh_options)
if warehouse != "（全部）":
    filtered_base = filtered_base[filtered_base["仓库代码"] == warehouse]

# ========= 自提仓库筛选 =========
pickup_options = filtered_base["自提仓库"].dropna().unique().tolist()
pickup = st.selectbox("选择自提仓库（可选）", options=["（全部）"] + pickup_options)
if pickup != "（全部）":
    filtered_base = filtered_base[filtered_base["自提仓库"] == pickup]


# ========= 表格内勾选（支持锁定工作流）=========
st.markdown("### 📋 勾选要发往自提仓的运单号（支持多选）")

# 初始化锁定态
if "sel_locked" not in st.session_state:
    st.session_state.sel_locked = False
if "locked_df" not in st.session_state:
    st.session_state.locked_df = pd.DataFrame()
if "selected_rows" not in st.session_state:
    st.session_state.selected_rows = set()

# 基础表用于展示
table = filtered_base[base_cols].sort_values(
    by=["ETA", "运单号"], na_position="last"
).reset_index(drop=True)

# ============ 未锁定状态：可勾选 + 可改“自提仓库” ============
if not st.session_state.sel_locked:
    table["选择"] = table["运单号"].astype(str).isin(st.session_state.selected_rows)

    col1, col2, col3 = st.columns([0.3, 0.3, 0.4])
    with col1:
        if st.button("✅ 全选当前列表"):
            st.session_state.selected_rows.update(table["运单号"].astype(str))
            table["选择"] = True
    with col2:
        if st.button("❌ 全不选当前列表"):
            st.session_state.selected_rows.difference_update(table["运单号"].astype(str))
            table["选择"] = False
    with col3:
        if st.button("🔄 反选当前列表"):
            current_ids = set(table["运单号"].astype(str))
            st.session_state.selected_rows.symmetric_difference_update(current_ids)
            table["选择"] = table["运单号"].astype(str).isin(st.session_state.selected_rows)

    with st.form("pick_ship_form", clear_on_submit=False):
        edited = st.data_editor(
            table,
            hide_index=True,
            use_container_width=True,
            height=420,
            column_config={
                "选择": st.column_config.CheckboxColumn("选择"),
                "ETA": st.column_config.DatetimeColumn("ETA", format="YYYY-MM-DD"),
                "自提仓库": st.column_config.SelectboxColumn(
                    "自提仓库",
                    options=sorted([x for x in wh_options if pd.notna(x)]),
                    help="第一段自提到的仓库（来自BOL自，可在此调整）。"
                ),
            },
            # 允许编辑「自提仓库」，其他保持禁用
            disabled=["仓库代码", "运单号", "客户单号", "ETA", "收费重"],
            key="ship_select_editor"
        )
        # 同步勾选
        st.session_state.selected_rows = set(edited.loc[edited["选择"], "运单号"].astype(str))
        submit_lock = st.form_submit_button("🔒 锁定选择并进入计算")

    if submit_lock:
        selected = edited[edited["选择"]].copy()
        if selected.empty:
            st.warning("请至少勾选一条再锁定。")
            st.stop()
        # 只保留必要列，进入锁定态
        st.session_state.locked_df = selected[base_cols].copy().reset_index(drop=True)
        st.session_state.sel_locked = True
        st.rerun()

# ============ 锁定状态：显示已锁定/未锁定，两块列表 ============
else:
    st.success("✅ 已锁定所选运单。")
    if st.button("🔓 重新选择"):
        st.session_state.sel_locked = False
        st.session_state.locked_df = pd.DataFrame()
        st.session_state.selected_rows = set()
        st.session_state.pop("ship_select_editor", None)
        st.rerun()

    # 已锁定清单（允许继续修改“自提仓库”）
    locked_df = st.session_state.locked_df.copy()
    locked_ids = set(locked_df["运单号"].astype(str))
    others_df = table[~table["运单号"].astype(str).isin(locked_ids)].copy()

    left, right = st.columns([1,1], gap="large")
    with left:
        st.markdown("**📦 已锁定（可继续调整“自提仓库”）**")
        edited_locked = st.data_editor(
            locked_df,
            hide_index=True,
            use_container_width=True,
            height=320,
            column_config={
                "ETA": st.column_config.DatetimeColumn("ETA", format="YYYY-MM-DD"),
                "自提仓库": st.column_config.SelectboxColumn(
                    "自提仓库",
                    options=sorted([x for x in wh_options if pd.notna(x)]),
                ),
            },
            disabled=["仓库代码", "运单号", "客户单号", "ETA", "收费重"],  # 若希望 ETA 也可改，把 "ETA" 从这里移除
            key="locked_editor"
        )
        # 将修改写回 session
        st.session_state.locked_df = edited_locked.copy()
        st.caption(f"已锁定数量：{len(edited_locked)}")

    with right:
        st.markdown("**🗂 未锁定（仅查看）**")
        others_df["选择"] = False
        st.dataframe(others_df[["选择"] + base_cols], use_container_width=True, height=320)
        st.caption(f"未锁定数量：{len(others_df)}")

    # 将 selected 指向锁定表，供后续统一计算使用
    selected = st.session_state.locked_df.copy()

# 若还未锁定或锁定后为空，终止后续计算
st.caption(f"当前选中 {len(st.session_state.get('selected_rows', []))} 条；锁定后参与计算 {len(st.session_state.locked_df) if st.session_state.sel_locked else 0} 条")
if (not st.session_state.sel_locked) or st.session_state.locked_df.empty:
    st.stop()

# ========= 录入卡车信息 & 费用 =========
st.markdown("### 🧾 车次信息")
c1, c2 = st.columns([2,2])
with c1:
    truck_no = st.text_input("卡车单号（必填）")
with c2:
    total_cost = st.number_input("本车总费用（必填）", min_value=0.0, step=1.0, format="%.2f")
if not truck_no or total_cost <= 0:
    st.info("请填写卡车单号与本车总费用。")
    st.stop()

# ========= 费用分摊 =========
if selected["收费重"].isna().any() or (selected["收费重"] <= 0).any():
    st.error("所选运单存在缺失或非正的『收费重』，无法分摊。")
    st.stop()
sum_wt = selected["收费重"].sum()
if sum_wt <= 0:
    st.error("总收费重为 0，无法分摊。")
    st.stop()

selected["分摊比例"] = selected["收费重"] / sum_wt
selected["分摊费用_raw"] = selected["分摊比例"] * total_cost
selected["分摊费用"] = selected["分摊费用_raw"].round(2)
diff = round(total_cost - selected["分摊费用"].sum(), 2)
if abs(diff) >= 0.01:
    selected.loc[selected.index[-1], "分摊费用"] += diff

# ========= 输出准备（含自提仓库）=========
out_df = selected.copy()
out_df["卡车单号"] = truck_no
out_df["总费用"] = round(float(total_cost), 2)
out_df["ETA"] = pd.to_datetime(out_df["ETA"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
out_df["分摊比例"] = (out_df["分摊比例"] * 100).round(2).astype(str) + "%"
out_df["分摊费用"] = out_df["分摊费用"].map(lambda x: f"{x:.2f}")
out_df["总费用"] = out_df["总费用"].map(lambda x: f"{x:.2f}")

preview_cols = ["卡车单号", "仓库代码", "自提仓库", "运单号", "客户单号",
                "ETA", "收费重", "分摊比例", "分摊费用", "总费用"]
for c in preview_cols:
    if c not in out_df.columns:
        out_df[c] = ""

st.markdown("### ✅ 上传预览")
st.dataframe(out_df[preview_cols], use_container_width=True, height=320)

# ========= 上传（不改表头，直接按现有表头顺序写入）=========
if st.button("📤 追加上传到『bol自提明细』"):
    try:
        ss = client.open(SHEET_SHIP_DETAIL)
        ship_sheet = ss.sheet1
    except SpreadsheetNotFound:
        st.error(f"找不到工作表「{SHEET_SHIP_DETAIL}」。")
        st.stop()

    existing = ship_sheet.get_all_values()
    if not existing:
        st.error("目标表为空且无表头。请先在表中设置表头。")
        st.stop()

    existing_header = existing[0]

    # ✅ 1) 强制要求目标表有「自提仓库」与「ETA(到自提仓)」
    must_have = {"自提仓库", "ETA(到自提仓)"}
    missing = [c for c in must_have if c not in existing_header]
    if missing:
        st.error(f"目标表缺少必需表头：{', '.join(missing)}。请在『bol自提明细』中添加这些列。")
        st.stop()

    # ✅ 2) 在上传副本里把 ETA → ETA(到自提仓)
    tmp = out_df.copy()
    if "ETA" in tmp.columns and "ETA(到自提仓)" not in tmp.columns:
        tmp.rename(columns={"ETA": "ETA(到自提仓)"}, inplace=True)

    # 可选：写入日期列
    if "日期" in existing_header and "日期" not in tmp.columns:
        tmp["日期"] = datetime.today().strftime("%Y-%m-%d")

    # ✅ 3) 对齐目标表头（缺的补空，多的丢弃）
    for col in existing_header:
        if col not in tmp.columns:
            tmp[col] = ""
    rows = tmp.reindex(columns=existing_header).fillna("").values.tolist()

    ship_sheet.append_rows(rows, value_input_option="USER_ENTERED")

    st.success(f"已上传 {len(rows)} 条到『{SHEET_SHIP_DETAIL}』。卡车单号：{truck_no}")
    st.cache_data.clear()
    st.session_state.pop("ship_select_editor", None)
    st.rerun()

