# recv_app.py  —— 收货托盘绑定（主数据源：bol自提明细 + 到仓数据表(箱数/仓库代码)）
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound
from datetime import datetime, timedelta, date
import random
import string
from uuid import uuid4


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
SHEET_ARRIVALS_NAME   = "到仓数据表"
SHEET_SHIP_DETAIL     = "bol自提明细"    # 发货app追加的源，作为收货展示主数据
SHEET_PALLET_DETAIL   = "托盘明细表"      # 收货端上传目标表（追加）

# ========= 工具函数 =========
def excel_serial_to_date(val):
    """把 Excel 数字日期(如 45857) 转为 datetime；非法返回 NaT"""
    try:
        f = float(val)
        return datetime(1899, 12, 30) + timedelta(days=f)
    except Exception:
        return pd.NaT

def generate_pallet_id():
    return "P" + ''.join(random.choices(string.digits, k=3))

# ========= 缓存读取 =========
@st.cache_data(ttl=60)
def load_ship_detail_df():
    """
    读取 bol自提明细（发货明细），作为收货展示的主数据源。
    只保留：运单号 / 客户单号 / ETA(到BCF)。日期可能是字符串或序列号，这里统一解析为 datetime。
    """
    try:
        ws = client.open(SHEET_SHIP_DETAIL).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame()

    vals = ws.get_all_values(value_render_option="UNFORMATTED_VALUE",
                             date_time_render_option="SERIAL_NUMBER")
    if not vals:
        return pd.DataFrame()

    header = vals[0]
    rows   = vals[1:]
    df = pd.DataFrame(rows, columns=header)

    # 兜底需要列
    for col in ["运单号", "客户单号", "ETA(到BCF)"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["运单号"] = df["运单号"].astype(str).str.strip()
    df = df[df["运单号"] != ""]

    # ETA 解析：尝试序列号，再 to_datetime
    parsed_serial = df["ETA(到BCF)"].apply(excel_serial_to_date)
    fallback      = pd.to_datetime(df["ETA(到BCF)"], errors="coerce")
    df["ETA(到BCF)"] = parsed_serial.combine_first(fallback)

    # 若同一运单出现多行（发货端可能多次追加），保留最后一条
    if not df.empty:
        df = df.groupby("运单号", as_index=False).last()

    return df[["运单号", "客户单号", "ETA(到BCF)"]]

@st.cache_data(ttl=60)
def load_arrivals_df():
    """
    读取 到仓数据表；仅保留：运单号 / 仓库代码 / 箱数。
    """
    ws = client.open(SHEET_ARRIVALS_NAME).sheet1
    data = ws.get_all_values()
    if not data:
        return pd.DataFrame()

    header = [h.replace("\u00A0", " ").replace("\n", "").replace(" ", "") for h in data[0]]
    df = pd.DataFrame(data[1:], columns=header)

    for need in ["运单号", "仓库代码", "箱数"]:
        if need not in df.columns:
            df[need] = pd.NA

    df["运单号"] = df["运单号"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["运单号"])
    # 箱数转数值（可能仍需人工调整）
    df["箱数"] = pd.to_numeric(df["箱数"], errors="coerce")

    return df[["运单号", "仓库代码", "箱数"]]

# ========= 页面设置 =========
st.set_page_config(page_title="物流收货平台（基于发货明细）", layout="wide")

st.title("📦 BCF 收货托盘绑定（数据源：bol自提明细 + 到仓箱数）")

# ========= 刷新缓存 =========
tools_l, _ = st.columns([1,6])
with tools_l:
    if st.button("🔄 刷新数据缓存"):
        st.cache_data.clear()
        st.rerun()

# ========= 初始化状态 =========
if "all_pallets" not in st.session_state:
    st.session_state["all_pallets"] = []
if "pallet_detail_records" not in st.session_state:
    st.session_state["pallet_detail_records"] = []

# ========= 数据加载 =========
ship_df    = load_ship_detail_df()   # 运单号 / 客户单号 / ETA(到BCF)
arrivals   = load_arrivals_df()      # 运单号 / 仓库代码 / 箱数

if ship_df.empty and arrivals.empty:
    st.warning("没有从 Google Sheets 读取到数据，请检查表名/权限。")
    st.stop()

# ========= 合并（以 bol自提明细 为主，左连到仓数据表的 仓库代码 / 箱数）=========
merged_df = ship_df.merge(arrivals, on="运单号", how="left")
# 确保 ETA(到BCF) 为 datetime
merged_df["ETA(到BCF)"] = pd.to_datetime(merged_df["ETA(到BCF)"], errors="coerce")

# ===== 日期筛选（按 ETA(到BCF)）=====
valid_dates = merged_df["ETA(到BCF)"].dropna()
if valid_dates.empty:
    st.warning("当前数据中没有可解析的 ETA(到BCF)。请检查源表或刷新缓存。")
    st.stop()

min_d = valid_dates.min().date()
max_d = valid_dates.max().date()
default_start = max(max_d - timedelta(days=14), min_d)

st.markdown("### 🔎 按 ETA(到BCF) 日期筛选")
start_date, end_date = st.date_input(
    "选择日期范围（包含端点）",
    value=(default_start, max_d),
    min_value=min_d,
    max_value=max_d
)

mask_date = merged_df["ETA(到BCF)"].between(pd.to_datetime(start_date), pd.to_datetime(end_date))
merged_df_by_date = merged_df[mask_date].copy()

# ===== 仓库筛选（基于日期过滤后的结果）=====
warehouse_options = merged_df_by_date["仓库代码"].dropna().unique()
if len(warehouse_options) == 0:
    st.warning("当前日期范围内没有仓库数据，请调整日期范围。")
    st.stop()

warehouse = st.selectbox("选择仓库代码：", warehouse_options)

# ===== 展示合并结果（已按日期与仓库过滤）=====
display_cols = ["仓库代码", "运单号", "客户单号", "ETA(到BCF)", "箱数"]
use_cols = [c for c in display_cols if c in merged_df_by_date.columns]
filtered_df = merged_df_by_date[merged_df_by_date["仓库代码"] == warehouse]
filtered_df = filtered_df[use_cols].sort_values(by=["ETA(到BCF)", "运单号"], na_position="last")

st.markdown("### 📋 已到 BCF 的待收货运单（已按日期与仓库过滤）")
st.dataframe(filtered_df, use_container_width=True, height=320)

# ========== 托盘绑定逻辑 ==========
st.markdown("### 🧰 托盘操作")
if st.button("➕ 新建托盘"):
    new_pallet = generate_pallet_id()
    while new_pallet in st.session_state["all_pallets"]:
        new_pallet = generate_pallet_id()
    st.session_state["all_pallets"].append(new_pallet)

for pallet_id in list(st.session_state["all_pallets"]):
    with st.expander(f"📦 托盘 {pallet_id} 操作区", expanded=True):
        st.markdown(f"🚚 当前托盘号：**{pallet_id}**")
        waybills = filtered_df["运单号"].dropna().unique()

        num_entries = st.number_input(
            f"添加运单数量 - 托盘 {pallet_id}",
            min_value=1, step=1, value=1, key=f"num_{pallet_id}"
        )

        st.markdown("#### 📦 托盘整体尺寸（统一填写一次）")
        pallet_cols = st.columns(4)
        with pallet_cols[0]:
            weight = st.number_input("托盘重量", min_value=0.0, key=f"weight_{pallet_id}")
        with pallet_cols[1]:
            length = st.number_input("托盘长", min_value=0.0, key=f"length_{pallet_id}")
        with pallet_cols[2]:
            width = st.number_input("托盘宽",  min_value=0.0, key=f"width_{pallet_id}")
        with pallet_cols[3]:
            height = st.number_input("托盘高",  min_value=0.0, key=f"height_{pallet_id}")

        st.markdown("#### 📦 运单明细（每单单独填写箱数）")
        entries = []
        for i in range(num_entries):
            cols = st.columns([3, 1])
            with cols[0]:
                wb = st.selectbox(f"运单号 {i+1}", waybills, key=f"wb_{pallet_id}_{i}")
            with cols[1]:
                qty = st.number_input("箱数", min_value=1, key=f"qty_{pallet_id}_{i}")
            entries.append((wb, qty))

        if st.button(f"🚀 确认绑定托盘 {pallet_id}"):
            is_merged = len(entries) > 1
            detail_type = "并板托盘" if is_merged else "普通托盘"

            for wb, qty in entries:
                row = filtered_df[filtered_df["运单号"] == wb].iloc[0]
                rid = f"{pallet_id}-{uuid4().hex[:6]}"
                record = {
                    "托盘号": pallet_id,
                    "仓库代码": warehouse,
                    "运单号": wb,
                    "客户单号": row.get("客户单号", ""),
                    "箱数": qty,
                    "重量": weight,
                    "长": length,
                    "宽": width,
                    "高": height,
                    "ETA(到BCF)": row.get("ETA(到BCF)", ""),
                    "类型": detail_type
                }
                st.session_state["pallet_detail_records"].append(record)

            st.success(f"✅ 托盘 {pallet_id} 绑定完成（{detail_type}）")
            st.session_state["all_pallets"].remove(pallet_id)

# ======= SUBMIT 按钮放大加粗高亮样式 =======
st.markdown("""
    <style>
    /* 针对上传区的 SUBMIT 按钮放大 + 高亮 */
    div.stButton > button[kind="secondary"] {
        font-size: 28px !important;      /* 字体很大 */
        font-weight: 900 !important;     /* 加粗 */
        padding: 0.8em 1.6em !important; /* 内边距大一点 */
        background-color: #ff9800 !important; /* 醒目橙色背景 */
        color: white !important;         /* 白色文字 */
        border-radius: 10px !important;  /* 圆角 */
        border: 3px solid #e65100 !important; /* 深色边框 */
    }
    </style>
""", unsafe_allow_html=True)


# ========== 展示与编辑托盘明细（本地内存，可删除/自动保存编辑）==========
if st.session_state["pallet_detail_records"]:
    st.markdown("### 📦 当前托盘明细记录（上传前可编辑/删除）")

    df_preview = pd.DataFrame(st.session_state["pallet_detail_records"]).copy()

    # 惯用列顺序
    base_cols = ["托盘号", "仓库代码", "运单号", "客户单号",
                 "箱数", "重量", "长", "宽", "高", "ETA(到BCF)", "类型"]
    for col in base_cols:
        if col not in df_preview.columns:
            df_preview[col] = ""

    df_preview = df_preview[base_cols]

    # 把“删除”放到最后一列
    if "删除" not in df_preview.columns:
        df_preview["删除"] = False
    else:
        df_preview["删除"] = df_preview["删除"].astype(bool)

    edited_df = st.data_editor(
        df_preview,
        key="preview_editor",
        num_rows="fixed",
        use_container_width=True,
        height=360,
        column_config={
            "托盘号": st.column_config.TextColumn(disabled=True),
            "仓库代码": st.column_config.TextColumn(disabled=True),
            "运单号": st.column_config.TextColumn(disabled=True),
            "客户单号": st.column_config.TextColumn(),
            "箱数": st.column_config.NumberColumn(step=1, min_value=1),
            "重量": st.column_config.NumberColumn(),
            "长": st.column_config.NumberColumn(),
            "宽": st.column_config.NumberColumn(),
            "高": st.column_config.NumberColumn(),
            "ETA(到BCF)": st.column_config.DatetimeColumn(),
            "类型": st.column_config.TextColumn(disabled=True),
            "删除": st.column_config.CheckboxColumn("删除"),
        },
    )

    # 自动保存编辑
    updated_records = edited_df.drop(columns=["删除"], errors="ignore").to_dict(orient="records")
    st.session_state["pallet_detail_records"] = updated_records

    # 删除按钮
    cdel, _, _ = st.columns([1, 1, 6])
    with cdel:
        if st.button("🗑️ 删除所选"):
            to_delete_idx = edited_df.index[edited_df["删除"] == True].tolist()
            if to_delete_idx:
                kept = [r for i, r in enumerate(updated_records) if i not in to_delete_idx]
                st.session_state["pallet_detail_records"] = kept
                st.success(f"已删除 {len(to_delete_idx)} 条记录")
                st.rerun()
            else:
                st.info("未勾选要删除的记录。")

    st.markdown("---")

    # ========== 上传托盘明细到 Google Sheets ==========
    c1, c2, _ = st.columns([2, 2, 6])
    with c1:
        clear_after = st.checkbox("上传后清空本地记录", value=True)
    with c2:
        if st.button("📤 SUBMIT"):
            df_upload = pd.DataFrame(st.session_state["pallet_detail_records"]).copy()

            rename_map = {"重量": "托盘重量", "长": "托盘长", "宽": "托盘宽", "高": "托盘高"}
            df_upload.rename(columns=rename_map, inplace=True)

            # 日期列转字符串
            dt_cols = df_upload.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns.tolist()
            if "ETA(到BCF)" in df_upload.columns and df_upload["ETA(到BCF)"].dtype == object:
                df_upload["ETA(到BCF)"] = pd.to_datetime(df_upload["ETA(到BCF)"], errors="coerce")
                if "ETA(到BCF)" not in dt_cols:
                    dt_cols.append("ETA(到BCF)")
            for c in dt_cols:
                df_upload[c] = df_upload[c].dt.strftime("%Y-%m-%d").fillna("")

            if "箱数" in df_upload.columns:
                df_upload["箱数"] = pd.to_numeric(df_upload["箱数"], errors="coerce").fillna(0).astype(int)

            try:
                ss = client.open(SHEET_PALLET_DETAIL)
                sheet = ss.sheet1
            except SpreadsheetNotFound:
                ss = client.create(SHEET_PALLET_DETAIL)
                sheet = ss.sheet1

            existing = sheet.get_all_values()
            if not existing:
                header = df_upload.columns.tolist()
                rows = df_upload.fillna("").values.tolist()
                sheet.update([header] + rows)
            else:
                existing_header = existing[0]
                tmp = df_upload.copy()
                for col in existing_header:
                    if col not in tmp.columns:
                        tmp[col] = ""
                rows = tmp.reindex(columns=existing_header).fillna("").values.tolist()
                sheet.append_rows(rows, value_input_option="USER_ENTERED")

            st.success(f"✅ 已追加上传 {len(df_upload)} 条托盘明细到「{SHEET_PALLET_DETAIL}」")

            if clear_after:
                st.session_state["pallet_detail_records"] = []
                st.rerun()
