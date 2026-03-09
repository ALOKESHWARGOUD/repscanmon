"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export interface PlatformBreakdown {
  platform: string;
  positive: number;
  negative: number;
  neutral: number;
  total: number;
}

// ── Platform helpers ──────────────────────────────────────────────────────────

const PLATFORM_ICONS: Record<string, string> = {
  youtube: "🎬",
  reddit: "🔴",
  twitter: "🐦",
  news: "📰",
};

function platformIcon(platform: string): string {
  return PLATFORM_ICONS[platform?.toLowerCase()] ?? "🌐";
}

// ── Custom Tooltip ────────────────────────────────────────────────────────────

interface TooltipEntry {
  name: string;
  value: number;
  color: string;
}

interface CustomTooltipProps {
  active?: boolean;
  payload?: TooltipEntry[];
  label?: string;
}

function CustomTooltip({ active, payload, label }: CustomTooltipProps) {
  if (!active || !payload?.length) return null;

  const total = payload.reduce((s, p) => s + (p.value ?? 0), 0);
  const icon = platformIcon(label ?? "");

  return (
    <div className="bg-slate-800 border border-slate-600/50 rounded-lg px-3 py-2.5 shadow-xl text-xs min-w-[170px]">
      <p className="text-slate-300 font-semibold mb-2">
        {icon} {label}
      </p>
      {payload.map((entry) => {
        const pct = total > 0 ? ((entry.value / total) * 100).toFixed(1) : "0";
        return (
          <div
            key={entry.name}
            className="flex items-center justify-between gap-4 mb-1"
          >
            <span className="flex items-center gap-1.5">
              <span
                className="h-2 w-2 rounded-sm flex-shrink-0"
                style={{ background: entry.color }}
              />
              <span className="text-slate-400 capitalize">{entry.name}</span>
            </span>
            <span className="font-bold tabular-nums" style={{ color: entry.color }}>
              {entry.value.toLocaleString()}{" "}
              <span className="text-slate-500 font-normal">({pct}%)</span>
            </span>
          </div>
        );
      })}
      <div className="border-t border-slate-600/50 mt-2 pt-2 flex justify-between">
        <span className="text-slate-500">Total</span>
        <span className="font-bold text-slate-300 tabular-nums">
          {total.toLocaleString()}
        </span>
      </div>
    </div>
  );
}

// ── Y-axis custom tick ────────────────────────────────────────────────────────

interface CustomYAxisTickProps {
  x?: number;
  y?: number;
  payload?: { value: string };
}

function CustomYAxisTick({ x = 0, y = 0, payload }: CustomYAxisTickProps) {
  if (!payload?.value) return null;
  const icon = platformIcon(payload.value);
  return (
    <text x={x} y={y} dy={4} textAnchor="end" fill="#94a3b8" fontSize={12}>
      {icon} {payload.value}
    </text>
  );
}

// ── Skeleton ──────────────────────────────────────────────────────────────────

function ChartSkeleton() {
  return (
    <div className="w-full h-full flex flex-col gap-4 animate-pulse p-4">
      {Array.from({ length: 4 }).map((_, i) => (
        <div key={i} className="flex items-center gap-3">
          <div className="h-3 w-20 bg-slate-700/50 rounded" />
          <div
            className="h-7 bg-slate-700/50 rounded"
            style={{ width: `${40 + i * 15}%` }}
          />
        </div>
      ))}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

interface PlatformBreakdownChartProps {
  data: PlatformBreakdown[];
}

export default function PlatformBreakdownChart({
  data,
}: PlatformBreakdownChartProps) {
  const hasData = data && data.length > 0;

  // Enrich labels with icons for display
  const chartData = hasData
    ? data.map((d) => ({
        ...d,
        label: `${platformIcon(d.platform)} ${d.platform}`,
      }))
    : [];

  return (
    <div className="w-full h-full min-h-[200px]">
      {!hasData ? (
        <ChartSkeleton />
      ) : (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            layout="vertical"
            data={chartData}
            margin={{ top: 4, right: 24, left: 8, bottom: 4 }}
            barSize={20}
          >
            <CartesianGrid
              strokeDasharray="3 3"
              stroke="rgba(148,163,184,0.1)"
              horizontal={false}
            />

            <XAxis
              type="number"
              tick={{ fill: "#94a3b8", fontSize: 11 }}
              axisLine={{ stroke: "rgba(148,163,184,0.15)" }}
              tickLine={false}
              tickFormatter={(v: number) =>
                v >= 1000 ? `${(v / 1000).toFixed(1)}k` : String(v)
              }
            />

            <YAxis
              type="category"
              dataKey="label"
              tick={{ fill: "#94a3b8", fontSize: 12 }}
              axisLine={false}
              tickLine={false}
              width={100}
            />

            <Tooltip
              content={<CustomTooltip />}
              cursor={{ fill: "rgba(148,163,184,0.06)" }}
            />

            <Legend
              wrapperStyle={{ paddingTop: "10px" }}
              formatter={(value: string) => (
                <span
                  style={{
                    color: "#94a3b8",
                    fontSize: 12,
                    textTransform: "capitalize",
                  }}
                >
                  {value}
                </span>
              )}
            />

            <Bar dataKey="positive" stackId="a" fill="#10B981" radius={[0, 0, 0, 0]} />
            <Bar dataKey="neutral" stackId="a" fill="#3B82F6" />
            <Bar
              dataKey="negative"
              stackId="a"
              fill="#EF4444"
              radius={[0, 4, 4, 0]}
            />
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
