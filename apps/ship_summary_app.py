# ship_summary_app.py —— BCF 发货汇总（按仓库，含两页签）
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound
from datetime import datetime, timedelta

# ====== 配置 ======
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
SHEET_ARRIVALS_NAME = "到仓数据表"      # 需含：运单号 / 仓库代码 / 箱数 / 体积 / 收费重
SHEET_SHIP_DETAIL   = "bol自提明细"     # 需含：运单号 / 分摊费用(或提货费用) / ETA(到BCF)
SHEET_WB_SUMMARY_NAME = "运单全链路汇总"  # ★ 将此改成你的总表名（Tab2使用）

# ====== 授权 ======
def get_gspread_client():
    if "gcp_service_account" in st.secrets:
        sa_info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("service_accounts.json", scopes=SCOPES)
    return gspread.authorize(creds)

client = get_gspread_client()

# ====== 工具 ======
def _norm_cols(cols):
    return [c.replace("\u00A0", "").replace("\n", "").strip() for c in cols]

def _to_num(s):
    return pd.to_numeric(s, errors="coerce")

def _parse_date_series(s):
    """支持 Excel 序列号或常见日期字符串 -> pandas.Timestamp"""
    def excel_serial_to_dt(v):
        try:
            f = float(v)
            return datetime(1899, 12, 30) + timedelta(days=f)
        except Exception:
            return pd.NaT
    s1 = s.apply(excel_serial_to_dt)
    s2 = pd.to_datetime(s, errors="coerce")
    return s1.combine_first(s2)

def _pick(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None

# ====== 读取：到仓数据表 ======
@st.cache_data(ttl=60)
def load_arrivals_df():
    ws = client.open(SHEET_ARRIVALS_NAME).sheet1
    data = ws.get_all_values(value_render_option="UNFORMATTED_VALUE", date_time_render_option="SERIAL_NUMBER")
    if not data:
        return pd.DataFrame()
    header = _norm_cols(data[0])
    df = pd.DataFrame(data[1:], columns=header)

    col_wb  = _pick(df.columns, ["运单号","Waybill","单号"])
    col_wh  = _pick(df.columns, ["仓库代码","仓库","库"])
    col_box = _pick(df.columns, ["箱数","箱子数量","箱子数"])
    col_cbm = _pick(df.columns, ["体积","CBM","体积CBM"])
    col_wt  = _pick(df.columns, ["收费重","计费重","重量","收费重KG","计费重KG"])

    if col_wb is None: df["运单号"] = pd.NA; col_wb = "运单号"
    if col_wh is None: df["仓库代码"] = pd.NA; col_wh = "仓库代码"
    if col_box is None: df["箱数"] = pd.NA; col_box = "箱数"
    if col_cbm is None: df["体积"] = pd.NA; col_cbm = "体积"
    if col_wt is None: df["收费重"] = pd.NA; col_wt = "收费重"

    df[col_box] = _to_num(df[col_box])
    df[col_cbm] = _to_num(df[col_cbm])
    df[col_wt]  = _to_num(df[col_wt])
    df[col_wb] = df[col_wb].astype(str).str.strip()

    df = df.drop_duplicates(subset=[col_wb])
    return df[[col_wb,col_wh,col_box,col_cbm,col_wt]].rename(columns={
        col_wb:"运单号", col_wh:"仓库代码", col_box:"箱数", col_cbm:"体积", col_wt:"收费重"
    })

# ====== 读取：bol自提明细（提货费用 & 提货日期） ======
@st.cache_data(ttl=60)
def load_ship_detail_df():
    try:
        ws = client.open(SHEET_SHIP_DETAIL).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame()
    data = ws.get_all_values(value_render_option="UNFORMATTED_VALUE", date_time_render_option="SERIAL_NUMBER")
    if not data:
        return pd.DataFrame()
    header = _norm_cols(data[0])
    df = pd.DataFrame(data[1:], columns=header)

    # 列名兼容
    if "运单号" not in df.columns: df["运单号"] = pd.NA
    fee_col = "分摊费用" if "分摊费用" in df.columns else ("提货费用" if "提货费用" in df.columns else None)
    if fee_col is None:
        df["分摊费用"] = pd.NA
        fee_col = "分摊费用"
    if "ETA(到BCF)" not in df.columns:
        df["ETA(到BCF)"] = pd.NA

    df["运单号"] = df["运单号"].astype(str).str.strip()
    df["提货费用"] = _to_num(df[fee_col])
    df["提货日期"] = _parse_date_series(df["ETA(到BCF)"])

    if "仓库代码" in df.columns:
        return df[["运单号","提货费用","提货日期","仓库代码"]]
    return df[["运单号","提货费用","提货日期"]]

# ====== 读取：运单全链路汇总（发货信息） ======
@st.cache_data(ttl=60)
def load_wb_summary_df():
    """从《运单全链路汇总》读取发货侧所需列，并做宽松列名兼容与类型清洗"""
    try:
        ws = client.open(SHEET_WB_SUMMARY_NAME).sheet1
    except SpreadsheetNotFound:
        return pd.DataFrame()
    vals = ws.get_all_values(
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER",
    )
    if not vals:
        return pd.DataFrame()
    header = _norm_cols(vals[0])
    df = pd.DataFrame(vals[1:], columns=vals[0])

    col_wb   = _pick(df.columns, ["运单号","Waybill"])
    col_wh   = _pick(df.columns, ["仓库代码","仓库"])
    col_fee  = _pick(df.columns, ["发走费用","发货费用","出仓费用","发车费用"])
    col_ship = _pick(df.columns, ["发走日期","发货日期","出仓日期"])
    col_arrb = _pick(df.columns, ["到BCF日期","到BCF日","BCF日期"])
    col_arrw = _pick(df.columns, ["到仓日期","到仓日","到仓(wh)"])
    col_box  = _pick(df.columns, ["箱数","箱子数"])
    col_cbm  = _pick(df.columns, ["体积","CBM","体积CBM"])
    col_wt   = _pick(df.columns, ["收费重","计费重","重量","收费重KG","计费重KG"])

    for need, col in [("运单号", col_wb), ("仓库代码", col_wh)]:
        if col is None: df[need] = ""
    df2 = df.rename(columns={
        (col_wb or "运单号"): "运单号",
        (col_wh or "仓库代码"): "仓库代码",
    })

    # 费用
    if col_fee is None: df2["发走费用"] = pd.NA
    else: df2["发走费用"] = _to_num(df[col_fee])

    # 日期
    def _parse_col(cname):
        if cname and cname in df.columns:
            return _parse_date_series(df[cname])
        return pd.Series([pd.NaT]*len(df2))
    df2["到BCF日期"] = _parse_col(col_arrb)
    df2["发走日期"] = _parse_col(col_ship)
    df2["到仓日期"] = _parse_col(col_arrw)

    # 数量型（若总表缺失，先留空，稍后从《到仓数据表》补齐）
    if col_box: df2["箱数"] = _to_num(df[col_box])
    else:       df2["箱数"] = pd.NA
    if col_cbm: df2["体积"] = _to_num(df[col_cbm])
    else:       df2["体积"] = pd.NA
    if col_wt:  df2["收费重"] = _to_num(df[col_wt])
    else:       df2["收费重"] = pd.NA

    # 规范
    df2["运单号"] = df2["运单号"].astype(str).str.strip()
    df2["仓库代码"] = df2["仓库代码"].astype(str).str.strip()
    df2 = df2[df2["运单号"] != ""].drop_duplicates(subset=["运单号"])
    return df2[["运单号","仓库代码","箱数","体积","收费重","发走费用","到BCF日期","发走日期","到仓日期"]]

# ====== UI ======
st.set_page_config(page_title="📦 BCF 发货汇总（按仓库）", layout="wide")
st.title("📦 BCF 发货汇总（按仓库）")

tab1, tab2 = st.tabs(["提货信息（按仓库）", "发货信息（按仓库）"])

# ---------------- Tab1：提货信息（按仓库） ----------------
with tab1:
    left, right = st.columns([1,6])
    with left:
        if st.button("🔄 刷新缓存", key="btn_refresh_pickup"):
            st.cache_data.clear()
            st.rerun()

    arrivals = load_arrivals_df()
    ship     = load_ship_detail_df()

    if arrivals.empty or ship.empty:
        st.warning("未读取到有效数据。请确认「到仓数据表」与「bol自提明细」存在且包含必需列。")
        st.stop()

    # 时间筛选（提货日期=ETA(到BCF)）
    valid_dates = ship["提货日期"].dropna(
    if valid_dates.empty:
        st.info("未检测到有效的『提货日期』，将展示全部记录。")
        ship_f = ship.copy()
    else:
        min_d, max_d = valid_dates.min().date(), valid_dates.max().date()
        default_start = max(max_d - timedelta(days=14), min_d)
        start_date, end_date = st.date_input(
            "选择时间范围（提货日期）",
            value=(default_start, max_d),
            min_value=min_d, max_value=max_d,
            key="date_pickup"
        )
        ship_f = ship[ship["提货日期"].between(pd.to_datetime(start_date), pd.to_datetime(end_date))].copy()

    if ship_f.empty:
        st.warning("时间筛选后无数据。")
        st.stop()

    # 合并费用到到仓信息
    merged = pd.merge(
        ship_f, arrivals,
        on="运单号", how="left"
    )
    # 仓库兜底
    if "仓库代码_x" in merged.columns and "仓库代码_y" in merged.columns:
        merged["仓库代码"] = merged["仓库代码_y"].fillna(merged["仓库代码_x"])
        merged.drop(columns=["仓库代码_x","仓库代码_y"], inplace=True)
    elif "仓库代码" not in merged.columns and "仓库代码" in ship_f.columns:
        merged["仓库代码"] = ship_f["仓库代码"]

    # 数值兜底
    for c in ["箱数","体积","收费重","提货费用"]:
        if c not in merged.columns: merged[c] = pd.NA
        merged[c] = _to_num(merged[c])

    # 仓库过滤（可选）
    wh_list = merged["仓库代码"].dropna().unique().tolist()
    wh_pick = st.multiselect("筛选仓库（可多选，留空=全部）", options=sorted(wh_list), key="wh_pickup")
    if wh_pick:
        merged = merged[merged["仓库代码"].isin(wh_pick)]
    if merged.empty:
        st.warning("筛选后无数据。")
        st.stop()

    # 汇总（按仓库）
    grp = merged.groupby("仓库代码", dropna=False).agg(
        箱数合计=("箱数","sum"),
        体积合计=("体积","sum"),
        收费重合计KG=("收费重","sum"),
        提货费用合计=("提货费用","sum"),
    ).reset_index()
    grp["提货费用/KG"] = grp.apply(
        lambda r: (r["提货费用合计"] / r["收费重合计KG"])
        if pd.notna(r["收费重合计KG"]) and r["收费重合计KG"]>0 else pd.NA,
        axis=1
    )

    # Grand Total
    grand = pd.DataFrame({
        "仓库代码": ["Grand Total"],
        "箱数合计": [grp["箱数合计"].sum(skipna=True)],
        "体积合计": [grp["体积合计"].sum(skipna=True)],
        "收费重合计KG": [grp["收费重合计KG"].sum(skipna=True)],
        "提货费用合计": [grp["提货费用合计"].sum(skipna=True)],
    })
    grand["提货费用/KG"] = (
        grand["提货费用合计"] / grand["收费重合计KG"]
        if grand.loc[0,"收费重合计KG"] and grand.loc[0,"收费重合计KG"]>0 else pd.NA
    )

    st.markdown("### 📊 汇总结果（按仓库｜提货）")
    show_df = pd.concat([grp, grand], ignore_index=True)

    def _fmt2(x): 
        return "" if pd.isna(x) else f"{x:,.2f}"
    fmt_df = show_df.copy()
    for c in ["箱数合计","体积合计","收费重合计KG","提货费用合计","提货费用/KG"]:
        if c in fmt_df.columns: fmt_df[c] = fmt_df[c].map(_fmt2)

    st.dataframe(fmt_df, use_container_width=True, height=420)

    with st.expander("🔍 查看用于汇总的明细（提货）"):
        cols = ["仓库代码","运单号","箱数","体积","收费重","提货费用","提货日期"]
        exist_cols = [c for c in cols if c in merged.columns]
        st.dataframe(
            merged[exist_cols].sort_values(["仓库代码","提货日期","运单号"], na_position="last"),
            use_container_width=True, height=360
        )

    csv = show_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("⬇️ 下载汇总 CSV（提货）", data=csv, file_name="bcf_warehouse_pickup_summary.csv", mime="text/csv")

# ---------------- Tab2：发货信息（按仓库） ----------------
with tab2:
    left, right = st.columns([1,6])
    with left:
        if st.button("🔄 刷新缓存", key="btn_refresh_ship"):
            st.cache_data.clear()
            st.rerun()

    wb_sum = load_wb_summary_df()
    arrivals2 = load_arrivals_df()

    if wb_sum.empty:
        st.warning(f"未能从『{SHEET_WB_SUMMARY_NAME}』读取到数据或缺少关键列。")
        st.stop()

    # 用《到仓数据表》补齐 箱数/体积/收费重（总表缺的情况下）
    if not arrivals2.empty:
        w = arrivals2[["运单号","仓库代码","箱数","体积","收费重"]].copy()
        w = w.drop_duplicates(subset=["运单号"])
        m = wb_sum.merge(w, on=["运单号","仓库代码"], how="left", suffixes=("","_arr"))
        for c in ["箱数","体积","收费重"]:
            if c in m.columns and f"{c}_arr" in m.columns:
                m[c] = m[c].combine_first(m[f"{c}_arr"])
        wb_sum = m[[ "运单号","仓库代码","箱数","体积","收费重","发走费用","到BCF日期","发走日期","到仓日期" ]]

    # 时间筛选（以“发走日期”为锚，更贴合发货侧运营周期）
    valid_ship = wb_sum["发走日期"].dropna()
    if valid_ship.empty:
        st.info("没有可用的『发走日期』，将展示全部记录。")
        wb_f = wb_sum.copy()
    else:
        min_d, max_d = valid_ship.min().date(), valid_ship.max().date()
        default_start = max(max_d - timedelta(days=14), min_d)
        start_date, end_date = st.date_input(
            "选择时间范围（发走日期）",
            value=(default_start, max_d),
            min_value=min_d, max_value=max_d,
            key="date_ship"
        )
        wb_f = wb_sum[wb_sum["发走日期"].between(pd.to_datetime(start_date), pd.to_datetime(end_date))].copy()

    if wb_f.empty:
        st.warning("时间筛选后无数据。")
        st.stop()

    # 仓库过滤（可选）
    wh_list2 = wb_f["仓库代码"].dropna().unique().tolist()
    wh_pick2 = st.multiselect("筛选仓库（可多选，留空=全部）", options=sorted(wh_list2), key="wh_ship")
    if wh_pick2:
        wb_f = wb_f[wb_f["仓库代码"].isin(wh_pick2)]
    if wb_f.empty:
        st.warning("筛选后无数据。")
        st.stop()

    # 数值兜底
    for c in ["箱数","体积","收费重","发走费用"]:
        wb_f[c] = _to_num(wb_f[c])

    # 时效（逐单）
    wb_f["_发货时效天"] = (wb_f["发走日期"] - wb_f["到BCF日期"]).dt.days     # 发走 - 到BCF
    wb_f["_妥投时效天"] = (wb_f["到仓日期"] - wb_f["发走日期"]).dt.days     # 到仓 - 发走

    # 按仓库汇总
    grp_ship = wb_f.groupby("仓库代码", dropna=False).agg(
        箱数合计=("箱数","sum"),
        体积合计=("体积","sum"),
        收费重合计KG=("收费重","sum"),
        发货费用合计=("发走费用","sum"),
        发货时效天=("_发货时效天","mean"),
        妥投时效天=("_妥投时效天","mean"),
        单据数=("运单号","count"),
    ).reset_index()

    # 发货费用/KG
    grp_ship["发货费用/KG"] = grp_ship.apply(
        lambda r: (r["发货费用合计"]/r["收费重合计KG"]) if pd.notna(r["收费重合计KG"]) and r["收费重合计KG"]>0 else pd.NA,
        axis=1
    )

    # Grand Total（时效按全量逐单平均）
    grand_ship = pd.DataFrame({
        "仓库代码": ["Grand Total"],
        "箱数合计": [grp_ship["箱数合计"].sum(skipna=True)],
        "体积合计": [grp_ship["体积合计"].sum(skipna=True)],
        "收费重合计KG": [grp_ship["收费重合计KG"].sum(skipna=True)],
        "发货费用合计": [grp_ship["发货费用合计"].sum(skipna=True)],
        "发货时效天": [wb_f["_发货时效天"].mean(skipna=True)],
        "妥投时效天": [wb_f["_妥投时效天"].mean(skipna=True)],
        "单据数": [wb_f["运单号"].count()],
    })
    grand_ship["发货费用/KG"] = (
        grand_ship["发货费用合计"]/grand_ship["收费重合计KG"]
        if grand_ship.loc[0,"收费重合计KG"] and grand_ship.loc[0,"收费重合计KG"]>0 else pd.NA
    )

    st.markdown("### 🚚 发货信息汇总（按仓库）")
    show_ship = pd.concat([grp_ship, grand_ship], ignore_index=True)
        # 调整列顺序：把「发货时效天」「妥投时效天」放在最后两列
    desired_order = [
        "仓库代码", "箱数合计", "体积合计", "收费重合计KG",
        "发货费用合计", "单据数", "发货费用/KG",
        "发货时效天", "妥投时效天"
    ]
    present = [c for c in desired_order if c in show_ship.columns]
    others  = [c for c in show_ship.columns if c not in present]
    show_ship = show_ship[present + others]

    def _fmt2(x): 
        return "" if pd.isna(x) else f"{x:,.2f}"
    fmt_cols = ["箱数合计","体积合计","收费重合计KG","发货费用合计","发货费用/KG","发货时效天","妥投时效天"]
    fmt_ship = show_ship.copy()
    for c in fmt_cols:
        if c in fmt_ship.columns: fmt_ship[c] = fmt_ship[c].map(_fmt2)

    st.dataframe(fmt_ship, use_container_width=True, height=420)

    with st.expander("🔍 查看用于汇总的明细（发货，含时效天数）"):
        detail = wb_f.copy()
        detail["发货时效天"] = detail["_发货时效天"]
        detail["妥投时效天"] = detail["_妥投时效天"]
        st.dataframe(
            detail[["仓库代码","运单号","箱数","体积","收费重","发走费用","到BCF日期","发走日期","到仓日期","发货时效天","妥投时效天"]]
                .sort_values(["仓库代码","发走日期","运单号"], na_position="last"),
            use_container_width=True, height=360
        )

    csv2 = show_ship.to_csv(index=False).encode("utf-8-sig")
    st.download_button("⬇️ 下载汇总 CSV（发货）", data=csv2, file_name="bcf_warehouse_ship_summary.csv", mime="text/csv")
