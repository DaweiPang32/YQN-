# ========== 计算阶段（基于已锁定的选择，同时显示未锁定的托盘）==========
if st.session_state.sel_locked:
    st.success("✅ 已锁定托盘选择")
    # 提供“重新选择”
    if st.button("🔓 重新选择"):
        st.session_state.sel_locked = False
        st.session_state.locked_df = pd.DataFrame()
        st.rerun()

    # 已锁定托盘
    selected_pal = st.session_state.locked_df.copy()
    # 其余未锁定托盘（只读展示）
    # 注意：这里依赖上文的 disp_df 和 cols_order（["选择"] + show_cols）
    locked_ids = set(selected_pal["托盘号"].astype(str))
    others_df = disp_df[~disp_df["托盘号"].astype(str).isin(locked_ids)].copy()
    # 只读表里把“选择”列固定为 False（避免误导）
    if "选择" in others_df.columns:
        others_df["选择"] = False

    # 两块并排展示：左=已锁定，右=未锁定（只读）
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

    # 选中数量 & 体积合计（只算已锁定）
    sel_count = int(len(selected_pal))
    sel_vol_sum = pd.to_numeric(selected_pal.get("托盘体积", pd.Series()), errors="coerce").sum()
    m1, m2 = st.columns(2)
    with m1: st.metric("已选择托盘数", sel_count)
    with m2: st.metric("选中体积合计（CBM）", round(float(sel_vol_sum or 0.0), 2))

    if sel_count == 0:
        st.info("当前没有锁定的托盘。点击『重新选择』返回。")
        st.stop()

    # 车次信息（分摊按“托盘重量”）——以下保持你原逻辑
    st.subheader("🧾 车次信息（托盘维度分摊）")
    cc1, cc2 = st.columns([2,2])
    with cc1:
        pallet_truck_no = st.text_input("卡车单号（必填）", key="pallet_truck_no")
    with cc2:
        pallet_total_cost = st.number_input("本车总费用（必填）", min_value=0.0, step=1.0, format="%.2f", key="pallet_total_cost")

    if not pallet_truck_no or pallet_total_cost <= 0:
        st.info("请填写卡车单号与本车总费用。")
        st.stop()

    # 分摊计算（按托盘重量）
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
