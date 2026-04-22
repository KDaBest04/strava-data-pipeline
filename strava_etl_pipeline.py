import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import numpy as np # Thêm thư viện này để xử lý null tốt hơn

load_dotenv()

CLIENT_ID = os.getenv('STRAVA_CLIENT_ID')
CLIENT_SECRET = os.getenv('STRAVA_CLIENT_SECRET')
REFRESH_TOKEN = os.getenv('STRAVA_REFRESH_TOKEN')
DATABASE_URL = os.getenv('NEON_DATABASE_URL')


def get_fresh_access_token():
    url = 'https://www.strava.com/oauth/token'
    payload = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': REFRESH_TOKEN,
        'grant_type': 'refresh_token'
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json().get('access_token')


def extract_latest_data(access_token):
    """Chỉ lấy 10 hoạt động gần nhất để tối ưu"""
    print("📥 Đang kiểm tra các buổi chạy mới nhất...")
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    # Chỉ lấy trang 1, 10 buổi gần nhất là đủ để không bỏ sót
    params = {"per_page": 10, "page": 1}

    response = requests.get(url, headers=headers, params=params)
    return pd.DataFrame(response.json())


def transform_data(df_raw):
    if df_raw.empty: return df_raw
    print("⚙️ Đang chuẩn hóa dữ liệu mới...")

    # GIỮ LẠI CỘT average_heartrate TỪ RAW DATA
    required_cols = ['id', 'name', 'distance', 'moving_time', 'type', 'start_date_local', 'average_speed', 'average_heartrate']
    # Chỉ lấy các cột tồn tại trong data thô (phòng trường hợp Strava không trả về)
    existing_cols = [c for c in required_cols if c in df_raw.columns]
    df = df_raw[existing_cols].copy()
    
    df = df[df['type'] == 'Run']  # Chỉ lấy chạy bộ

    if df.empty: return df

    # --- CHỐNG LỖI NULL average_speed (KHI CHẠY MÁY/NHẬP TAY) ---
    # Tự động tính vận tốc = quãng đường / thời gian nếu Strava trả về null
    df['average_speed'] = df['average_speed'].fillna(
        (df['distance'] / df['moving_time']).replace([np.inf, -np.inf], 0)
    )

    df['distance_km'] = (df['distance'] / 1000).round(2)
    df['duration_min'] = (df['moving_time'] / 60).round(2)

    def calc_pace(speed):
        if pd.isna(speed) or speed <= 0: return "00:00"
        seconds_per_km = 1000 / speed
        return f"{int(seconds_per_km // 60):02d}:{int(seconds_per_km % 60):02d}"

    df['pace'] = df['average_speed'].apply(calc_pace)
    df['run_date'] = pd.to_datetime(df['start_date_local']).dt.date
    
    # Đảm bảo nhịp tim là kiểu số nguyên (nullable)
    if 'average_heartrate' in df.columns:
        df['average_heartrate'] = pd.to_numeric(df['average_heartrate'], errors='coerce').astype('Int64')
    else:
        df['average_heartrate'] = None # Tạo cột rỗng nếu API không trả về

    return df[['id', 'name', 'run_date', 'distance_km', 'duration_min', 'pace', 'average_heartrate']]


def load_incremental(df_new):
    """Chiến thuật Upsert: Chỉ nạp dữ liệu nếu ID chưa tồn tại"""
    if df_new.empty:
        print("☕ Không có buổi chạy bộ mới nào.")
        return

    engine = create_engine(DATABASE_URL)

    # 1. Đẩy dữ liệu mới vào một bảng tạm (staging_table)
    print("⏳ Đang xử lý chống trùng lặp dữ liệu...")
    df_new.to_sql('staging_activities', engine, if_exists='replace', index=False)

    # 2. Dùng SQL để INSERT từ bảng tạm vào bảng chính, chỉ lấy những ID chưa có
    # --- CẬP NHẬT CÂU LỆNH SQL ĐỂ ĐẨY NHỊP TIM LÊN ---
    with engine.begin() as conn:
        query = text("""
                     INSERT INTO silver_activities (id, name, run_date, distance_km, duration_min, pace, average_heartrate)
                     SELECT s.id, s.name, s.run_date, s.distance_km, s.duration_min, s.pace, s.average_heartrate
                     FROM staging_activities s
                     WHERE NOT EXISTS (SELECT 1
                                       FROM silver_activities target
                                       WHERE target.id = s.id);
                     """)
        result = conn.execute(query)
        print(f"✅ Đã cập nhật thêm {result.rowcount} buổi chạy mới (có dữ liệu nhịp tim) vào Cloud!")

        # Xóa bảng tạm sau khi xong
        conn.execute(text("DROP TABLE staging_activities;"))


if __name__ == "__main__":
    try:
        token = get_fresh_access_token()
        raw_df = extract_latest_data(token)
        clean_df = transform_data(raw_df)
        load_incremental(clean_df)
    except Exception as e:
        print(f"❌ Pipeline gặp lỗi: {e}")
