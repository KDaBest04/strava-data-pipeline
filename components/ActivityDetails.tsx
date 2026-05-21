"use client";

import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

// Khai báo kiểu dữ liệu cho chuẩn TypeScript
interface StreamData {
  distance: number;
  time: number;
  cadence?: number;
  heartrate?: number;
  altitude?: number;
}

interface ActivityDetailsProps {
  streamData: StreamData[];
}

// Hàm format Pace (giây/km -> MM:SS)
const formatPace = (secsPerKm: number) => {
  if (!secsPerKm || !isFinite(secsPerKm)) return "00:00";
  const mins = Math.floor(secsPerKm / 60);
  const secs = Math.floor(secsPerKm % 60);
  return `${mins}:${secs.toString().padStart(2, '0')}`;
};

// Hàm gom nhóm dữ liệu từng giây thành từng Km
const calculateSplits = (streamData: StreamData[]) => {
  if (!streamData || streamData.length === 0) return [];

  const lapsMap: Record<number, any> = {};

  streamData.forEach(row => {
    const kmLap = Math.floor(row.distance / 1000) + 1;

    if (!lapsMap[kmLap]) {
      lapsMap[kmLap] = {
        kmLap,
        time_start: row.time, time_end: row.time,
        dist_start: row.distance, dist_end: row.distance,
        hr_sum: 0, cad_sum: 0, count: 0,
        alt_start: row.altitude || 0, alt_end: row.altitude || 0
      };
    }

    const lap = lapsMap[kmLap];
    lap.time_end = row.time;
    lap.dist_end = row.distance;
    lap.hr_sum += row.heartrate || 0;
    lap.cad_sum += row.cadence || 0;
    if (row.altitude) lap.alt_end = row.altitude;
    lap.count++;
  });

  return Object.values(lapsMap).map(lap => {
    const lap_dist_m = lap.dist_end - lap.dist_start;
    const lap_time_s = lap.time_end - lap.time_start;

    if (lap_dist_m < 100) return null;

    const paceSecPerKm = lap_dist_m > 0 ? lap_time_s / (lap_dist_m / 1000) : 0;

    return {
      vong: `Km ${lap.kmLap}`,
      paceVong: formatPace(paceSecPerKm),
      khoangCach: (lap_dist_m / 1000).toFixed(2) + " km",
      nhipTim: Math.round(lap.hr_sum / lap.count),
      guongChan: Math.round(lap.cad_sum / lap.count),
      doCao: (lap.alt_end - lap.alt_start).toFixed(1) + " m"
    };
  }).filter(Boolean);
};

export default function ActivityDetails({ streamData }: ActivityDetailsProps) {
  if (!streamData || streamData.length === 0) {
    return (
      <div className="text-yellow-600 bg-yellow-50 border border-yellow-200 p-4 rounded-lg mt-4 text-sm font-medium">
        ⚠️ Buổi chạy này chưa có dữ liệu chi tiết từng giây (Stream).
      </div>
    );
  }

  const laps = calculateSplits(streamData);

  return (
    <div className="space-y-8 mt-6">
      {/* Biểu đồ Guồng Chân */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
        <h3 className="text-lg font-bold text-gray-800 mb-4">Guồng Chân Từng Giây (Cadence)</h3>
        <div className="h-64 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={streamData}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} opacity={0.5} />
              <XAxis
                dataKey="distance"
                tickFormatter={(val) => `${(val / 1000).toFixed(1)}km`}
                minTickGap={30}
              />
              <YAxis domain={[130, 200]} />
              <Tooltip
                labelFormatter={(val) => `Quãng đường: ${val}m`}
                // Xóa (val: number) và thay bằng (val: any) như bên dưới:
                formatter={(val: any) => [val, 'Nhịp chân']}
              />
              <Line type="monotone" dataKey="cadence" stroke="#ffaa00" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
      {/* 2. Biểu đồ Nhịp tim (Zone 2 Tracking) */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
        <h3 className="text-lg font-bold text-gray-800 mb-4 flex items-center">
          <span className="bg-red-100 p-2 rounded-lg mr-3">❤️</span> Nhịp Tim (Heart Rate)
        </h3>
        <div className="h-64 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={streamData}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} opacity={0.5} />
              <XAxis dataKey="distance" tickFormatter={(val) => `${(val / 1000).toFixed(1)}km`} minTickGap={30} />
              {/* Vùng nhịp tim thường từ 100 - 190 */}
              <YAxis domain={[100, 190]} />
              <Tooltip labelFormatter={(val) => `Quãng đường: ${val}m`} formatter={(val: any) => [val, 'BPM']} />
              <Line type="monotone" dataKey="heartrate" stroke="#ef4444" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* 3. Biểu đồ Độ cao (Elevation) */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
        <h3 className="text-lg font-bold text-gray-800 mb-4 flex items-center">
          <span className="bg-green-100 p-2 rounded-lg mr-3">⛰️</span> Biến Thiên Độ Cao (Altitude)
        </h3>
        <div className="h-64 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={streamData}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} opacity={0.5} />
              <XAxis dataKey="distance" tickFormatter={(val) => `${(val / 1000).toFixed(1)}km`} minTickGap={30} />
              <YAxis domain={['auto', 'auto']} />
              <Tooltip labelFormatter={(val) => `Quãng đường: ${val}m`} formatter={(val: any) => [val, 'Độ cao (m)']} />
              <Line type="monotone" dataKey="altitude" stroke="#10b981" strokeWidth={2} fill="#d1fae5" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Bảng Dữ Liệu Từng Vòng */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
        <div className="p-6 border-b border-gray-100 bg-gray-50/50">
          <h3 className="text-lg font-bold text-gray-800">Dữ Liệu Từng Vòng (Splits/Km)</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm text-gray-600">
            <thead className="bg-gray-50 text-gray-900 font-semibold border-b border-gray-100">
              <tr>
                <th className="px-6 py-4">Vòng</th>
                <th className="px-6 py-4">Pace Vòng</th>
                <th className="px-6 py-4">Khoảng Cách</th>
                <th className="px-6 py-4">Nhịp Tim</th>
                <th className="px-6 py-4">Guồng Chân</th>
                <th className="px-6 py-4">Độ Cao</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {laps.map((lap: any, idx: number) => (
                <tr key={idx} className="hover:bg-blue-50/50 transition-colors">
                  <td className="px-6 py-4 font-bold text-gray-900">{lap.vong}</td>
                  <td className="px-6 py-4 font-mono text-blue-600 font-medium">{lap.paceVong}</td>
                  <td className="px-6 py-4">{lap.khoangCach}</td>
                  <td className="px-6 py-4">{lap.nhipTim} bpm</td>
                  <td className="px-6 py-4">{lap.guongChan} spm</td>
                  <td className="px-6 py-4">{lap.doCao}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}