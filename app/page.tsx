"use client";
import React, { useState, useEffect, useMemo } from 'react';
import AnalysisTab from '../components/AnalysisTab';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Line, ComposedChart, Legend } from 'recharts';

export default function Dashboard() {
  const [activeTab, setActiveTab] = useState('overview');
  const [filter, setFilter] = useState('all');
  const [activities, setActivities] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/activities').then(res => res.json()).then(data => {
      setActivities(data);
      setLoading(false);
    });
  }, []);

  // 1. Lọc dữ liệu theo thời gian
  const filtered = useMemo(() => {
    const now = new Date();
    return activities.filter(a => {
      const runDate = new Date(a.run_date);
      if (filter === 'week') {
        const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        return runDate >= weekAgo;
      }
      if (filter === 'month') {
        return runDate.getMonth() === now.getMonth() && runDate.getFullYear() === now.getFullYear();
      }
      return true;
    });
  }, [activities, filter]);

  // 2. Tính toán KPI (Đã xử lý làm tròn số)
  const stats = useMemo(() => {
    return {
      dist: filtered.reduce((s, a) => s + Number(a.distance_km || 0), 0).toFixed(1),
      time: Math.round(filtered.reduce((s, a) => s + Number(a.duration_min || 0), 0)),
      count: filtered.length,
      kcal: Math.round(filtered.reduce((s, a) => s + Number(a.calories || 0), 0)).toLocaleString(),
      elev: Math.round(filtered.reduce((s, a) => s + Number(a.total_elevation_gain || 0), 0)),
    };
  }, [filtered]);

  // 3. Logic xử lý dữ liệu cho Biểu đồ xu hướng (Trend Chart)
  const chartData = useMemo(() => {
    const dataMap: Record<string, any> = {};

    filtered.forEach(a => {
      const d = new Date(a.run_date);
      let label = "";

      if (filter === 'week') label = d.toLocaleDateString('vi-VN', {weekday: 'short'}); // Thứ 2, Thứ 3...
      else if (filter === 'month') label = `Tuần ${Math.ceil(d.getDate() / 7)}`;
      else label = `T${d.getMonth() + 1}`; // Tháng 1, 2...

      if (!dataMap[label]) dataMap[label] = { label, distance: 0, time: 0 };
      dataMap[label].distance += Number(a.distance_km || 0);
      dataMap[label].time += Number(a.duration_min || 0);
    });

    return Object.values(dataMap).map(item => ({
      ...item,
      distance: Number(item.distance.toFixed(1)),
      time: Math.round(item.time)
    }));
  }, [filtered, filter]);

  if (loading) return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
       <div className="animate-spin rounded-full h-12 w-12 border-t-4 border-orange-600 border-gray-200"></div>
    </div>
  );

  return (
    <div className="min-h-screen bg-[#f8f9fa] p-4 md:p-8 text-gray-900">
      <div className="max-w-6xl mx-auto">

        {/* LOGO MỚI - ĐẸP & TO HƠN */}
        <header className="flex flex-col md:flex-row md:items-center justify-between mb-10 gap-6">
          <div className="flex items-center gap-4">
            <div className="bg-gradient-to-br from-orange-500 to-red-600 p-3 rounded-2xl shadow-lg shadow-orange-200">
              <svg width="32" height="32" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M13 20L19 4L5 14L11 12L13 20Z" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
            </div>
            <div>
              <h1 className="text-3xl font-black italic tracking-tighter bg-clip-text text-transparent bg-gradient-to-r from-gray-900 to-gray-600">
                RUNNER PRO <span className="text-orange-600">DASHBOARD</span>
              </h1>
              <div className="h-1 w-20 bg-orange-500 rounded-full mt-1"></div>
            </div>
          </div>

          <div className="flex bg-white p-1.5 rounded-2xl border-2 border-gray-100 shadow-sm">
            <button
              onClick={() => setActiveTab('overview')}
              className={`px-8 py-2.5 rounded-xl font-black text-sm transition-all ${activeTab === 'overview' ? 'bg-gray-900 text-white shadow-md' : 'text-gray-400 hover:text-gray-600'}`}
            >TỔNG QUAN</button>
            <button
              onClick={() => setActiveTab('analysis')}
              className={`px-8 py-2.5 rounded-xl font-black text-sm transition-all ${activeTab === 'analysis' ? 'bg-gray-900 text-white shadow-md' : 'text-gray-400 hover:text-gray-600'}`}
            >PHÂN TÍCH</button>
          </div>
        </header>

        {activeTab === 'overview' ? (
          <div className="space-y-8 animate-in fade-in duration-500">
            {/* Filter Buttons */}
            <div className="flex gap-3">
              {['all', 'week', 'month'].map(f => (
                <button
                  key={f}
                  onClick={() => setFilter(f)}
                  className={`px-6 py-2 rounded-xl text-xs font-black border-2 transition-all ${filter === f ? 'bg-orange-600 border-orange-600 text-white shadow-lg shadow-orange-200' : 'bg-white border-gray-100 text-gray-500 hover:border-gray-300'}`}
                >
                  {f === 'all' ? 'TẤT CẢ' : f === 'week' ? 'TUẦN NÀY' : 'THÁNG NÀY'}
                </button>
              ))}
            </div>

            {/* KPI Cards (Đã sửa lỗi số thập phân) */}
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
              {[
                { label: "Khoảng cách", val: stats.dist, unit: "km", color: "text-blue-600" },
                { label: "Thời gian", val: stats.time, unit: "phút", color: "text-gray-900" },
                { label: "Hoạt động", val: stats.count, unit: "buổi", color: "text-emerald-600" },
                { label: "Tổng Kcal", val: stats.kcal, unit: "kcal", color: "text-orange-500" },
                { label: "Độ cao", val: stats.elev, unit: "m", color: "text-indigo-600" },
                { label: "Tải (Load)", val: "650", unit: "pts", color: "text-red-500" },
              ].map((s, i) => (
                <div key={i} className="bg-white p-5 rounded-2xl border-b-4 border-gray-100 hover:border-orange-500 shadow-sm transition-all">
                  <p className="text-[10px] font-black text-gray-400 uppercase tracking-widest mb-1">{s.label}</p>
                  <div className="flex items-baseline gap-1">
                    <span className={`text-2xl font-black ${s.color}`}>{s.val}</span>
                    <span className="text-[10px] font-bold text-gray-400">{s.unit}</span>
                  </div>
                </div>
              ))}
            </div>

            {/* TREND CHART - BIỂU ĐỒ XU HƯỚNG MỚI */}
            <div className="bg-white p-8 rounded-[32px] border border-gray-100 shadow-sm">
              <div className="flex justify-between items-center mb-8">
                <div>
                  <h3 className="text-xl font-black text-gray-900">XU HƯỚNG LUYỆN TẬP</h3>
                  <p className="text-sm text-gray-400 font-bold">Quãng đường (km) & Thời gian (phút)</p>
                </div>
                <div className="flex gap-4">
                   <div className="flex items-center gap-2 text-xs font-bold text-gray-600">
                      <div className="w-3 h-3 bg-blue-500 rounded-sm"></div> Quãng đường
                   </div>
                   <div className="flex items-center gap-2 text-xs font-bold text-gray-600">
                      <div className="w-3 h-3 bg-orange-500 rounded-full"></div> Thời gian
                   </div>
                </div>
              </div>

              <div className="h-80 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <ComposedChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                    <XAxis dataKey="label" stroke="#9ca3af" fontSize={12} tickLine={false} axisLine={false} />
                    <YAxis yAxisId="left" stroke="#9ca3af" fontSize={12} tickLine={false} axisLine={false} />
                    <YAxis yAxisId="right" orientation="right" stroke="#9ca3af" fontSize={12} tickLine={false} axisLine={false} />
                    <Tooltip
                       contentStyle={{borderRadius: '16px', border: 'none', boxShadow: '0 10px 15px -3px rgba(0,0,0,0.1)', fontWeight: 'bold'}}
                    />
                    <Bar yAxisId="left" dataKey="distance" fill="#3b82f6" radius={[6, 6, 0, 0]} barSize={40} />
                    <Line yAxisId="right" type="monotone" dataKey="time" stroke="#f97316" strokeWidth={4} dot={{r: 6, fill: '#f97316', strokeWidth: 2, stroke: '#fff'}} />
                  </ComposedChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        ) : (
          <AnalysisTab activities={activities} />
        )}
      </div>
    </div>
  );
}