// Vị trí: app/api/activities/route.ts

import { NextResponse } from 'next/server';
import { neon } from '@neondatabase/serverless';

export async function GET() {
  try {
    const sql = neon(process.env.DATABASE_URL!);

    // Câu lệnh SQL lấy danh sách (không có relative_effort)
    const activities = await sql`
      SELECT 
        id, name, run_date, distance_km, duration_min, 
        pace, average_heartrate, calories, total_elevation_gain
      FROM silver_activities 
      ORDER BY run_date DESC
    `;

    return NextResponse.json(activities);
  } catch (error) {
    console.error("Database Error:", error);
    return NextResponse.json({ error: "Failed to fetch data" }, { status: 500 });
  }
}