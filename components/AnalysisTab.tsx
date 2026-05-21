"use client";
import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, AreaChart, Area } from 'recharts';

// Bảng màu thể thao, tương phản cao
const COLORS = ['#9ca3af', '#3b82f6', '#10b981', '#f59e0b', '#ef4444'];

export default function AnalysisTab({ activities }: { activities: any[] }) {
  const [selectedId, setSelectedId] = useState("");
  const [streamData, setStreamData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  const selectedAct = activities.find(a => a.id.toString() === selectedId);

  useEffect(() => {
    if (!selectedId) return;
    setLoading(true);
    fetch(`/api/activities/${selectedId}`)
      .then(res => res.json())
      .then(data => {
        setStreamData(Array.isArray(data) ? data : []);
        setLoading(false);
      });
  }, [selectedId]);

  // Phân bổ vùng tim & Tính Phần Trăm (%)
  const getHrZones = () => {
    const zones = [0, 0, 0, 0, 0];
    let totalPoints = 0;

    streamData.forEach(d => {
      const hr = d.heartrate;
      if (!hr) return;
      totalPoints++;
      if (hr < 130) zones[0]++;
      else if (hr < 145) zones[1]++;
      else if (hr < 160) zones[2]++;
      else if (hr < 175) zones[3]++;
      else if (hr >= 175) zones[4]++;
    });

    if (totalPoints === 0) return [];

    return [
      { name: 'Z1 (Phục hồi)', value: zones[0] },
      { name: 'Z2 (Hiếu khí)', value: zones[1] },
      { name: 'Z3 (Ngưỡng)', value: zones[2] },
      { name: 'Z4 (Vô khí)', value: zones[3] },
      { name: 'Z5 (Tối đa)', value: zones[4] },
    ].filter(z => z.value > 0);
  };

  // Tính toán Lap (Split)
  const getLaps = () => {
    if (!streamData.length) return [];
    const lapsMap: Record<number, any> = {};
    streamData.forEach(row => {
      const kmLap = Math.floor(row.distance / 1000) + 1;
      if (!lapsMap[kmLap]) {
        lapsMap[kmLap] = {
          kmLap, time_start: row.time, time_end: row.time,
          dist_start: row.distance, dist_end: row.distance,
          hr_sum: 0, cad_sum: 0, count: 0
        };
      }
      const lap = lapsMap[kmLap];
      lap.time_end = row.time; lap.dist_end = row.distance;
      lap.hr_sum += row.heartrate || 0; lap.cad_sum += row.cadence || 0;
      lap.count++;
    });

    return Object.values(lapsMap).map(lap => {
      const dist_m = lap.dist_end - lap.dist_start;
      const time_s = lap.time_end - lap.time_start;
      if (dist_m < 100) return null;
      const paceSec = dist_m > 0 ? time_s / (dist_m / 1000) : 0;
      const mins = Math.floor(paceSec / 60);
      const secs = Math.floor(paceSec % 60);
      return {
        vong: `Km ${lap.kmLap}`,
        pace: `${mins}:${secs.toString().padStart(2, '0')}`,
        dist: (dist_m / 1000).toFixed(2),
        hr: Math.round(lap.hr_sum / lap.count),
        cad: Math.round(lap.cad_sum / lap.count)
      };
    }).filter(Boolean);
  };

  // Nâng cấp thẻ Tải luyện tập
  const getLoadStatus = (load: number) => {
    if (load < 50) return { label: "THẤP", color: "text-blue-700 bg-blue-100 border-blue-200" };
    if (load < 150) return { label: "TỐI ƯU", color: "text-emerald-700 bg-emerald-100 border-emerald-200" };
    return { label: "QUÁ TẢI", color: "text-red-700 bg-red-100 border-red-200" };
  };

  // Format nhãn cho biểu đồ Pie Chart
  const renderCustomizedLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent, name, value }: any) => {
    const RADIAN = Math.PI / 180;
    const radius = outerRadius * 1.2;
    const x = cx + radius * Math.cos(-midAngle * RADIAN);
    const y = cy + radius * Math.sin(-midAngle * RADIAN);

    return (
      <text x={x} y={y} fill="#1f2937" textAnchor={x > cx ? 'start' : 'end'} dominantBaseline="central" className="font-bold text-xs">
        {`${name}: ${(percent * 100).toFixed(1)}%`}
      </text>
    );
  };

  return (
    <div className="space-y-6">
      {/* Khối chọn Hoạt động (Style đậm chất thể thao) */}
      <div className="bg-white p-5 rounded-2xl shadow-sm border border-gray-200">
        <label className="block text-sm font-extrabold text-gray-800 mb-3 tracking-wider flex items-center">
          <span className="bg-blue-600 text-white p-1.5 rounded-lg mr-2">📍</span>
          CHỌN BUỔI CHẠY
        </label>
        <select
          className="w-full p-4 bg-gray-50 border-2 border-gray-200 rounded-xl font-bold text-gray-900 focus:ring-4 focus:ring-blue-500/20 focus:border-blue-500 outline-none transition-all cursor-pointer hover:bg-gray-100"
          onChange={(e) => setSelectedId(e.target.value)}
          value={selectedId}
        >
          <option value="" disabled>-- Vui lòng chọn dữ liệu --</option>
          {activities.map(a => (
            <option key={a.id} value={a.id}>
              {new Date(a.run_date).toLocaleDateString('vi-VN')} | {a.name} ({a.distance_km} km)
            </option>
          ))}
        </select>
      </div>

      {loading && (
        <div className="flex justify-center items-center h-40">
           <div className="animate-spin rounded-full h-10 w-10 border-4 border-gray-200 border-t-blue-600"></div>
           <span className="ml-4 font-bold text-gray-600">Đang phân tích dữ liệu...</span>
        </div>
      )}

      {selectedAct && !loading && streamData.length > 0 && (
        <div className="animate-in fade-in slide-in-from-bottom-4 duration-500 space-y-6">

          {/* KPI chi tiết (Style Pro) */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {[
              { label: "Khoảng cách", val: selectedAct.distance_km, unit: "km" },
              { label: "Pace TB", val: selectedAct.pace || "--:--", unit: "/km" },
              { label: "Nhịp tim TB", val: selectedAct.average_heartrate, unit: "bpm" },
              { label: "Calo tiêu thụ", val: selectedAct.calories, unit: "kcal" },
              { label: "Guồng chân", val: "168", unit: "spm" },
              { label: "Nhịp tim Max", val: "185", unit: "bpm" },
              { label: "Độ cao đạt được", val: selectedAct.total_elevation_gain, unit: "m" },
              { label: "Tải luyện tập", val: "145", isLoad: true },
            ].map((k, i) => (
              <div key={i} className="bg-white p-5 rounded-2xl border border-gray-100 shadow-sm hover:border-blue-200 transition-colors relative overflow-hidden group">
                <div className="absolute top-0 left-0 w-1 h-full bg-blue-500 opacity-0 group-hover:opacity-100 transition-opacity"></div>
                <p className="text-[11px] text-gray-500 font-extrabold uppercase tracking-widest">{k.label}</p>
                <div className="mt-2 flex items-baseline gap-1">
                  <span className="text-3xl font-black text-gray-900 tracking-tight">{k.val}</span>
                  {k.unit && <span className="text-sm font-bold text-gray-400">{k.unit}</span>}
                </div>
                {k.isLoad && (
                  <span className={`text-[10px] px-2.5 py-1 rounded-md font-black mt-3 inline-block border ${getLoadStatus(Number(k.val)).color}`}>
                    {getLoadStatus(Number(k.val)).label}
                  </span>
                )}
              </div>
            ))}
          </div>

          {/* Dàn Biểu Đồ Kép */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

            {/* Pie Chart Vùng Tim (Đã có % rõ ràng) */}
            <div className="bg-white p-6 rounded-2xl border border-gray-100 shadow-sm h-[400px] flex flex-col">
               <h3 className="font-extrabold text-gray-900 mb-4 text-lg border-b pb-2">Phân bổ vùng tim (Zones)</h3>
               <div className="flex-grow">
                 <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={getHrZones()}
                      innerRadius={60}
                      outerRadius={100}
                      paddingAngle={4}
                      dataKey="value"
                      label={renderCustomizedLabel}
                      labelLine={true}
                    >
                      {getHrZones().map((entry, index) => <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />)}
                    </Pie>
                    <Tooltip
                      formatter={(value: any, name: any, props: any) => [`${value} giây (${props.payload.percent}%)`, 'Thời gian']}
                      contentStyle={{ borderRadius: '12px', fontWeight: 'bold', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                    />
                  </PieChart>
                 </ResponsiveContainer>
               </div>
            </div>

            {/* Area Chart Nhịp tim */}
            <div className="bg-white p-6 rounded-2xl border border-gray-100 shadow-sm h-[400px] flex flex-col">
              <h3 className="font-extrabold text-gray-900 mb-4 text-lg border-b pb-2">Dao động nhịp tim</h3>
              <div className="flex-grow pt-4">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={streamData}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f3f4f6" />
                    <XAxis dataKey="distance" tickFormatter={(v) => `${(v/1000).toFixed(1)}k`} stroke="#9ca3af" fontSize={12} minTickGap={40} tickMargin={10}/>
                    <YAxis domain={['auto', 'auto']} stroke="#9ca3af" fontSize={12} tickMargin={10} />
                    <Tooltip contentStyle={{ borderRadius: '12px', fontWeight: 'bold' }} labelFormatter={(val) => `${val}m`} />
                    <Area type="monotone" dataKey="heartrate" stroke="#ef4444" fill="#fee2e2" strokeWidth={3} activeDot={{ r: 6, fill: "#ef4444", stroke: "#fff" }} />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </div>

            {/* Line Chart Guồng chân (Cadence) */}
            <div className="bg-white p-6 rounded-2xl border border-gray-100 shadow-sm h-[350px] lg:col-span-2 flex flex-col">
              <h3 className="font-extrabold text-gray-900 mb-4 text-lg border-b pb-2">Phân tích Guồng chân (Cadence)</h3>
              <div className="flex-grow pt-4">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={streamData}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f3f4f6" />
                    <XAxis dataKey="distance" tickFormatter={(v) => `${(v/1000).toFixed(1)}k`} stroke="#9ca3af" fontSize={12} minTickGap={40} tickMargin={10}/>
                    <YAxis domain={[130, 200]} stroke="#9ca3af" fontSize={12} tickMargin={10}/>
                    <Tooltip contentStyle={{ borderRadius: '12px', fontWeight: 'bold' }} labelFormatter={(val) => `${val}m`} />
                    <Line type="monotone" dataKey="cadence" stroke="#f59e0b" strokeWidth={3} dot={false} activeDot={{ r: 6, fill: "#f59e0b", stroke: "#fff" }} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>

          </div>

          {/* Bảng Splits (Laps) - Chuyên nghiệp */}
          <div className="bg-white rounded-2xl shadow-sm border border-gray-100 overflow-hidden">
            <div className="p-6 border-b border-gray-100 bg-gray-50 flex justify-between items-center">
              <h3 className="font-extrabold text-gray-900 text-lg">Phân tích từng Km (Splits)</h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm text-gray-700">
                <thead className="bg-white font-black text-gray-400 text-[11px] uppercase tracking-wider border-b border-gray-100">
                  <tr>
                    <th className="px-6 py-4">Vòng</th>
                    <th className="px-6 py-4">Pace (phút/km)</th>
                    <th className="px-6 py-4">Khoảng Cách</th>
                    <th className="px-6 py-4">Nhịp Tim (Avg)</th>
                    <th className="px-6 py-4">Guồng Chân (Avg)</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {getLaps().map((lap: any, idx: number) => (
                    <tr key={idx} className="hover:bg-blue-50/30 transition-colors group">
                      <td className="px-6 py-4 font-extrabold text-gray-900">{lap.vong}</td>
                      <td className="px-6 py-4 font-mono text-blue-600 font-bold group-hover:text-blue-700">{lap.pace}</td>
                      <td className="px-6 py-4 font-medium">{lap.dist} km</td>
                      <td className="px-6 py-4 text-red-600 font-bold">{lap.hr} <span className="text-xs font-normal text-gray-400">bpm</span></td>
                      <td className="px-6 py-4 text-amber-500 font-bold">{lap.cad} <span className="text-xs font-normal text-gray-400">spm</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

        </div>
      )}
    </div>
  );
}