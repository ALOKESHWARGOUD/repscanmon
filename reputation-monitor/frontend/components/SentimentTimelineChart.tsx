"use client";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { format, parseISO } from "date-fns";

export interface TimelineDataPoint {
  hour: string;
  positive: number;
  negative: number;
  neutral: number;
}

// ── Skeleton ──────────────────────────────────────────────────────────────────

function ChartSkeleton() {
  return (
    <div className="w-full h-full flex flex-col gap-3 animate-pulse p-2">
      <div className="flex items-end gap-2 flex-1">
        {Array.from({ length: 12 }).map((_, i) => (
          <div
            key={i}
            className="flex-1 bg-slate-700/50 rounded-sm"
            style={{ height: `${30 + Math.random() * 60}%` }}
          />
        ))}
      </div>
      <div className="h-3 w-48 bg-slate-700/50 rounded mx-auto" />
    </div>
  );
}

// ── Custom Tooltip ────────────────────────────────────────────────────────────

interface TooltipPayloadEntry {
  name: string;
  value: number;
  color: string;
}

interface CustomTooltipProps {
  active?: boolean;
  payload?: TooltipPayloadEntry[];
  label?: string;
}

function CustomTooltip({ active, payload, label }: CustomTooltipProps) {
  if (!active || !payload?.length) return null;

  const formattedLabel = (() => {
    try {
      return format(parseISO(label ?? ""), "MMM d, HH:mm");
    } catch {
      return label ?? "";
    }
  })();

  const total = payload.reduce((sum, p) => sum + (p.value ?? 0), 0);

  return (
    <div className="bg-slate-800 border border-slate-600/50 rounded-lg px-3 py-2.5 shadow-xl text-xs min-w-[160px]">
      <p className="text-slate-400 font-medium mb-2">{formattedLabel}</p>
      {payload.map((entry) => (
        <div key={entry.name} className="flex items-center justify-between gap-4 mb-1">
          <span className="flex items-center gap-1.5">
            <span
              className="h-2 w-2 rounded-full flex-shrink-0"
              style={{ background: entry.color }}
            />
            <span className="text-slate-300 capitalize">{entry.name}</span>
          </span>
          <span className="font-bold tabular-nums" style={{ color: entry.color }}>
            {entry.value.toLocaleString()}
          </span>
        </div>
      ))}
      <div className="border-t border-slate-600/50 mt-2 pt-2 flex justify-between">
        <span className="text-slate-500">Total</span>
        <span className="font-bold text-slate-300 tabular-nums">
          {total.toLocaleString()}
        </span>
      </div>
    </div>
  );
}

// ── Axis formatter ────────────────────────────────────────────────────────────

function formatXAxis(value: string): string {
  try {
    const d = parseISO(value);
    return format(d, "HH:mm");
  } catch {
    return value;
  }
}

// ── Main component ────────────────────────────────────────────────────────────

interface SentimentTimelineChartProps {
  data: TimelineDataPoint[];
}

export default function SentimentTimelineChart({
  data,
}: SentimentTimelineChartProps) {
  const hasData = data && data.length > 0;

  return (
    <div className="w-full h-full min-h-[220px]">
      {!hasData ? (
        <ChartSkeleton />
      ) : (
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={data}
            margin={{ top: 8, right: 16, left: -8, bottom: 0 }}
          >
            <defs>
              <filter id="glow-green">
                <feGaussianBlur stdDeviation="2" result="blur" />
                <feMerge>
                  <feMergeNode in="blur" />
                  <feMergeNode in="SourceGraphic" />
                </feMerge>
              </filter>
              <filter id="glow-red">
                <feGaussianBlur stdDeviation="2" result="blur" />
                <feMerge>
                  <feMergeNode in="blur" />
                  <feMergeNode in="SourceGraphic" />
                </feMerge>
              </filter>
            </defs>

            <CartesianGrid
              strokeDasharray="3 3"
              stroke="rgba(148,163,184,0.1)"
              vertical={false}
            />

            <XAxis
              dataKey="hour"
              tickFormatter={formatXAxis}
              tick={{ fill: "#94a3b8", fontSize: 11 }}
              axisLine={{ stroke: "rgba(148,163,184,0.15)" }}
              tickLine={false}
              interval="preserveStartEnd"
            />

            <YAxis
              tick={{ fill: "#94a3b8", fontSize: 11 }}
              axisLine={false}
              tickLine={false}
              width={36}
              tickFormatter={(v: number) =>
                v >= 1000 ? `${(v / 1000).toFixed(1)}k` : String(v)
              }
            />

            <Tooltip
              content={<CustomTooltip />}
              cursor={{
                stroke: "rgba(148,163,184,0.2)",
                strokeWidth: 1,
                strokeDasharray: "4 4",
              }}
            />

            <Legend
              wrapperStyle={{ paddingTop: "12px" }}
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

            <Line
              type="monotone"
              dataKey="positive"
              stroke="#10B981"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: "#10B981", stroke: "#064e3b", strokeWidth: 2 }}
              filter="url(#glow-green)"
            />
            <Line
              type="monotone"
              dataKey="negative"
              stroke="#EF4444"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: "#EF4444", stroke: "#450a0a", strokeWidth: 2 }}
              filter="url(#glow-red)"
            />
            <Line
              type="monotone"
              dataKey="neutral"
              stroke="#3B82F6"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: "#3B82F6", stroke: "#1e3a8a", strokeWidth: 2 }}
              strokeDasharray="5 3"
            />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
