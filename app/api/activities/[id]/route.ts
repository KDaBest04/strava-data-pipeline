import { NextRequest, NextResponse } from 'next/server';
import { neon } from '@neondatabase/serverless';

export async function GET(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  try {
    const sql = neon(process.env.DATABASE_URL!);
    const resolvedParams = await params;

    const streams = await sql`
      SELECT * FROM silver_activity_streams 
      WHERE activity_id = ${resolvedParams.id}::bigint
      ORDER BY time ASC
    `;
    return NextResponse.json(streams);
  } catch (error) {
    return NextResponse.json({ error: "Failed" }, { status: 500 });
  }
}