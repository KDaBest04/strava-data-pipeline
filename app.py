import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
DATABASE_URL = os.getenv('NEON_DATABASE_URL')

st.set_page_config(page_title="Pro Runner Dashboard", layout="wide", page_icon="🏃‍♂️")


def format_pace(speed_m_s):
    if pd.isna(speed_m_s) or speed_m_s <= 0: return "00:00"
    secs_per_km = 1000 / speed_m_s
    return f"{int(secs_per_km // 60):02d}:{int(secs_per_km % 60):02d}"


def format_duration(minutes):
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    return f"{hours}h {mins}m" if hours > 0 else f"{mins}m"


@st.cache_data(ttl=300)
def load_summary_data():
    engine = create_engine(DATABASE_URL)
    query = "SELECT * FROM silver_activities ORDER BY run_date DESC"
    df = pd.read_sql(query, engine)
    df['run_date'] = pd.to_datetime(df['run_date']).dt.date

    if 'calories' not in df.columns:
        df['calories'] = 0.0
    if 'total_elevation_gain' not in df.columns:
        df['total_elevation_gain'] = 0.0
    if 'relative_effort' not in df.columns:
        df['relative_effort'] = (df['duration_min'] * (df['average_heartrate'] / 150) * 1.2).round(1)
    return df


@st.cache_data(ttl=300)
def load_stream_data(activity_id):
    engine = create_engine(DATABASE_URL)
    query = f"SELECT * FROM silver_activity_streams WHERE activity_id = {activity_id} ORDER BY time ASC"
    df = pd.read_sql(query, engine)

    if 'velocity_smooth' in df.columns:
        df['pace_smooth'] = df['velocity_smooth'].apply(lambda v: (1000 / v / 60) if v > 0.5 else np.nan)
        df['pace_smooth'] = df['pace_smooth'].rolling(window=15, min_periods=1).mean()

        if 'grade_smooth' in df.columns:
            df['gap_factor'] = df['grade_smooth'].apply(
                lambda g: 1 + 9 * (g / 100) + 12 * ((g / 100) ** 2) if not pd.isna(g) else 1)
            df['gap_velocity'] = df['velocity_smooth'] * df['gap_factor']
            df['gap_smooth'] = df['gap_velocity'].apply(lambda v: (1000 / v / 60) if v > 0.5 else np.nan)
            df['gap_smooth'] = df['gap_smooth'].rolling(window=15, min_periods=1).mean()

    return df


try:
    df_activities = load_summary_data()
    data_loaded = True
except Exception as e:
    st.error(f"Lỗi kết nối: {e}")
    data_loaded = False

if data_loaded and not df_activities.empty:
    st.title("Nhật Ký Tập Luyện")

    main_tab1, main_tab2 = st.tabs(["THỐNG KÊ TỔNG QUAN", "PHÂN TÍCH HOẠT ĐỘNG"])

    with main_tab1:
        filter_col, _ = st.columns([1, 3])
        with filter_col:
            time_filter = st.radio("Bộ lọc thời gian:", ["Tuần này", "Tháng này", "Tất cả"], horizontal=True)

        today = datetime.today().date()
        if time_filter == "Tuần này":
            start_date = today - timedelta(days=today.weekday())
            df_filtered = df_activities[df_activities['run_date'] >= start_date]
            group_freq = 'D'
        elif time_filter == "Tháng này":
            start_date = today.replace(day=1)
            df_filtered = df_activities[df_activities['run_date'] >= start_date]
            group_freq = 'D'
        else:
            df_filtered = df_activities
            group_freq = 'W'

        st.markdown("### Chỉ số tích lũy")
        kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)

        total_dist = df_filtered['distance_km'].sum()
        total_dur = df_filtered['duration_min'].sum()
        total_runs = len(df_filtered)
        total_load = df_filtered['relative_effort'].sum()
        total_calories = df_filtered['calories'].sum()
        total_gain = df_filtered['total_elevation_gain'].sum()

        kpi1.metric("Tổng Khoảng Cách", f"{total_dist:.2f} km")
        kpi2.metric("Thời Gian Chạy", format_duration(total_dur))
        kpi3.metric("Số Hoạt Động", f"{total_runs} buổi")
        kpi4.metric("Tổng Tiêu Hao", f"{total_calories:,.0f} kcal")
        kpi5.metric("Tổng Độ Cao Leo", f"{total_gain:.0f} m")
        kpi6.metric("Tổng Tải Luyện Tập", f"{total_load:.0f}")

        st.markdown("---")

        st.markdown(f"### Biểu đồ số KM theo {'Ngày' if group_freq == 'D' else 'Tuần'}")
        if not df_filtered.empty:
            df_filtered['date_group'] = pd.to_datetime(df_filtered['run_date'])
            df_bar = df_filtered.groupby(pd.Grouper(key='date_group', freq=group_freq))[
                'distance_km'].sum().reset_index()
            df_bar = df_bar[df_bar['distance_km'] > 0]

            fig_bar = px.bar(df_bar, x='date_group', y='distance_km', text_auto='.1f',
                             labels={'date_group': 'Thời gian', 'distance_km': 'Khoảng cách (km)'},
                             color_discrete_sequence=['#ff4b4b'])
            fig_bar.update_layout(xaxis_tickformat='%d/%m/%Y' if group_freq == 'D' else 'Tuần %W - %Y')
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Không có dữ liệu trong khoảng thời gian này.")

    with main_tab2:
        run_options = {f"{row['run_date'].strftime('%d/%m/%Y')} - {row['name']} ({row['distance_km']}km)": row for
                       _, row in df_activities.iterrows()}
        selected_label = st.selectbox("Chọn hoạt động để xem chi tiết:", list(run_options.keys()))
        selected_run = run_options[selected_label]
        act_id = selected_run['id']

        st.markdown(f"### Tổng quan: {selected_run['name']}")

        row1_1, row1_2, row1_3, row1_4, row1_5 = st.columns(5)
        row1_1.metric("Khoảng cách", f"{selected_run['distance_km']} km")
        row1_2.metric("Pace Trung Bình", selected_run['pace'])
        row1_3.metric("Nhịp Tim TB", f"{selected_run['average_heartrate']} bpm")
        row1_4.metric("Calo Tiêu Thụ", f"{selected_run['calories']:.0f} kcal")
        row1_5.metric("Độ Cao Leo (Gain)", f"{selected_run['total_elevation_gain']:.1f} m")

        st.markdown(" ")

        df_stream = load_stream_data(act_id)

        row2_1, row2_2, row2_3, row2_4, row2_5 = st.columns(5)

        max_hr = df_stream['heartrate'].max() if not df_stream.empty and 'heartrate' in df_stream.columns else None
        max_cad = df_stream['cadence'].max() if not df_stream.empty and 'cadence' in df_stream.columns else None

        row2_1.metric("Tốc độ TB", f"{selected_run['average_speed']:.2f} m/s")
        row2_2.metric("Guồng chân TB", f"{selected_run['average_cadence'] or '--'} spm")
        row2_3.metric("Nhịp Tim Max", f"{max_hr:.0f} bpm" if pd.notna(max_hr) else "--")
        row2_4.metric("Guồng Chân Max", f"{max_cad:.0f} spm" if pd.notna(max_cad) else "--")

        load_val = selected_run['relative_effort']
        if load_val < 50:
            load_status, color = "Thấp (Phục hồi)", "#00cc66"
        elif load_val < 100:
            load_status, color = "Trung Bình (Duy trì)", "#ffaa00"
        else:
            load_status, color = "Cao (Cải thiện)", "#ff4b4b"

        row2_5.markdown(
            f"**Tải Luyện Tập**<br><span style='color:{color}; font-size:1.2rem; font-weight:bold;'>{load_val:.0f} - {load_status}</span>",
            unsafe_allow_html=True)

        st.markdown("---")

        if not df_stream.empty:
            st.markdown("### Đồ Thị Cảm Biến Theo Thời Gian")

            if 'pace_smooth' in df_stream.columns:
                fig_pace = go.Figure()
                fig_pace.add_trace(go.Scatter(x=df_stream['distance'], y=df_stream['pace_smooth'],
                                              mode='lines', name='Pace thực tế',
                                              line=dict(color='#00a0ff', width=2)))

                if 'gap_smooth' in df_stream.columns:
                    fig_pace.add_trace(go.Scatter(x=df_stream['distance'], y=df_stream['gap_smooth'],
                                                  mode='lines', name='GAP (Pace đã chỉnh dốc)',
                                                  line=dict(color='#0055ff', width=1.5, dash='dash')))

                fig_pace.update_layout(title="Tốc độ & Tốc độ điều chỉnh theo độ dốc (GAP)",
                                       xaxis=dict(title="Quãng đường (m)"),
                                       yaxis=dict(title="Pace (phút/km)", autorange="reversed"))
                st.plotly_chart(fig_pace, use_container_width=True)

            if 'heartrate' in df_stream.columns:
                fig_hr = px.line(df_stream, x='distance', y='heartrate',
                                 title="Nhịp Tim (BPM)",
                                 labels={'distance': 'Quãng đường (m)', 'heartrate': 'Nhịp tim'},
                                 color_discrete_sequence=['#ff4b4b'])
                fig_hr.add_hline(y=150, line_dash="dot", line_color="green", annotation_text="Ngưỡng Zone 2")
                st.plotly_chart(fig_hr, use_container_width=True)

            if 'cadence' in df_stream.columns and df_stream['cadence'].max() > 0:
                fig_cad = px.line(df_stream, x='distance', y='cadence',
                                  title="Guồng Chân Từng Giây (Cadence)",
                                  labels={'distance': 'Quãng đường (m)', 'cadence': 'Nhịp chân'},
                                  color_discrete_sequence=['#ffaa00'])
                fig_cad.update_yaxes(range=[130, 200])
                st.plotly_chart(fig_cad, use_container_width=True)

            st.markdown("---")

            st.markdown("### Dữ Liệu Từng Vòng (Splits/Km)")
            if 'distance' in df_stream.columns:
                df_stream['km_lap'] = (df_stream['distance'] // 1000).astype(int) + 1

                agg_dict = {
                    'time_start': ('time', 'min'),
                    'time_end': ('time', 'max'),
                    'dist_start': ('distance', 'min'),
                    'dist_end': ('distance', 'max'),
                    'avg_hr': ('heartrate', 'mean'),
                    'avg_cad': ('cadence', 'mean')
                }

                if 'altitude' in df_stream.columns:
                    agg_dict['alt_start'] = ('altitude', 'first')
                    agg_dict['alt_end'] = ('altitude', 'last')

                laps = df_stream.groupby('km_lap').agg(**agg_dict).reset_index()

                laps['lap_dist_m'] = laps['dist_end'] - laps['dist_start']
                laps['lap_time_s'] = laps['time_end'] - laps['time_start']
                laps = laps[laps['lap_dist_m'] > 100].copy()

                laps['pace'] = laps['lap_time_s'] / (laps['lap_dist_m'] / 1000)
                laps['Pace Vòng'] = laps['pace'].apply(lambda x: format_pace(1000 / x) if x > 0 else "00:00")

                laps['Vòng'] = laps['km_lap'].apply(lambda x: f"Km {x}")
                laps['Khoảng Cách'] = (laps['lap_dist_m'] / 1000).round(2).astype(str) + " km"
                laps['Nhịp Tim'] = laps['avg_hr'].round(0).astype('Int64')
                laps['Guồng Chân'] = laps['avg_cad'].round(0).astype('Int64')

                if 'alt_start' in laps.columns:
                    laps['Độ Cao'] = (laps['alt_end'] - laps['alt_start']).round(1).astype(str) + " m"
                    display_cols = ['Vòng', 'Pace Vòng', 'Khoảng Cách', 'Nhịp Tim', 'Guồng Chân', 'Độ Cao']
                else:
                    display_cols = ['Vòng', 'Pace Vòng', 'Khoảng Cách', 'Nhịp Tim', 'Guồng Chân']

                display_laps = laps[display_cols].set_index('Vòng')
                st.dataframe(display_laps, use_container_width=True)

        else:
            st.warning("Buổi chạy này không có dữ liệu chi tiết từng giây (Stream).")
