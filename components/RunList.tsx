"use client";

import React, { useState } from 'react';
import ActivityDetails from './ActivityDetails';

export default function RunList({ activities }: { activities: any[] }) {
  const [selectedActivity, setSelectedActivity] = useState<any | null>(null);
  const [streamData, setStreamData] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const handleSelectChange = async (event: React.ChangeEvent<HTMLSelectElement>) => {
    const activityId = event.target.value;

    if (!activityId) {
      setSelectedActivity(null);
      setStreamData([]);
      return;
    }

    // Tìm thông tin buổi chạy từ danh sách tổng
    const activity = activities.find(a => a.id.toString() === activityId);
    setSelectedActivity(activity);
    setStreamData([]);
    setErrorMsg(null);
    setIsLoading(true);

    try {
      const res = await fetch(`/api/activities/${activity.id}`);
      if (!res.ok) throw new Error(`API báo lỗi mã: ${res.status}`);
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setStreamData(data);
    } catch (error: any) {
      setErrorMsg(error.message || "Không thể tải dữ liệu.");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="space-y-8">
      {/* Dropdown Chọn Buổi Chạy */}
      <div className="bg-white p-6 rounded-2xl shadow-sm border border-gray-100">
        <label htmlFor="activity-select" className="block text-sm font-bold text-gray-700 mb-3 uppercase tracking-wider">
          🔍 Chọn buổi chạy để phân tích
        </label>
        <select
          id="activity-select"
          className="w-full p-4 border-2 border-gray-200 rounded-xl shadow-sm focus:ring-blue-500 focus:border-blue-500 text-gray-800 font-medium bg-gray-50 cursor-pointer transition-all hover:bg-white"
          onChange={handleSelectChange}
          defaultValue=""
        >
          <option value="" disabled>-- Bấm vào đây để chọn lịch sử chạy --</option>
          {activities.map((act) => (
            <option key={act.id} value={act.id}>
              {new Date(act.run_date).toLocaleDateString('vi-VN')} | {act.name} - {(act.distance_km || 0).toFixed(2)} km - {act.average_heartrate} bpm
            </option>
          ))}
        </select>
      </div>

      {/* Khu vực hiển thị Phân tích chi tiết */}
      {selectedActivity && (
        <div className="bg-white p-8 rounded-2xl shadow-sm border border-gray-100 animate-in fade-in slide-in-from-top-4 duration-500">
          <div className="mb-8 pb-6 border-b border-gray-100">
            <h2 className="text-3xl font-extrabold text-gray-900 mb-2">
              {selectedActivity.name}
            </h2>
            <div className="flex gap-4 text-sm text-gray-600 font-medium mt-4 bg-gray-50 p-4 rounded-lg inline-flex">
              <span>📏 {(selectedActivity.distance_km || 0).toFixed(2)} km</span>
              <span>⏱️ {selectedActivity.duration_min} phút</span>
              <span>❤️ Avg HR: {selectedActivity.average_heartrate} bpm</span>
            </div>
          </div>

          {isLoading && (
            <div className="flex justify-center items-center h-40">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
              <span className="ml-3 text-gray-500 font-medium">Đang kéo dữ liệu từng giây từ Neon DB...</span>
            </div>
          )}

          {errorMsg && (
            <div className="text-red-600 bg-red-50 p-4 rounded-lg text-sm font-medium">❌ Lỗi: {errorMsg}</div>
          )}

          {!isLoading && !errorMsg && (
            <ActivityDetails streamData={streamData} />
          )}
        </div>
      )}
    </div>
  );
}