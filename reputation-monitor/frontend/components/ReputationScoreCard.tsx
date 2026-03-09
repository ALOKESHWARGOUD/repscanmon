"use client";

import { useMemo } from "react";
import { useCurrentScore } from "@/hooks/useKeywordData";
import { ReputationScore } from "@/lib/api";

// ── Gauge helpers ─────────────────────────────────────────────────────────────

const GAUGE_RADIUS = 70;
const STROKE_WIDTH = 10;
// Arc spans 240° (from 150° to 390° / -30° + 360°)
const ARC_DEGREES = 240;
const CIRCUMFERENCE = 2 * Math.PI * GAUGE_RADIUS;
const ARC_LENGTH = (ARC_DEGREES / 360) * CIRCUMFERENCE;

function scoreToColor(score: number): string {
  if (score > 20) return "#10B981";  // green
  if (score >= -20) return "#F59E0B"; // yellow
  return "#EF4444"; // red
}

function scoreToPercent(score: number): number {
  // Map score from [-100, 100] → [0, 1]
  return Math.min(1, Math.max(0, (score + 100) / 200));
}

// ── SVG Gauge ─────────────────────────────────────────────────────────────────

interface GaugeProps {
  score: number;
  color: string;
}

function CircularGauge({ score, color }: GaugeProps) {
  const size = (GAUGE_RADIUS + STROKE_WIDTH) * 2 + 4;
  const cx = size / 2;
  const cy = size / 2;

  const percent = scoreToPercent(score);
  const filled = percent * ARC_LENGTH;
  const empty = ARC_LENGTH - filled;

  // Start angle: 150° (bottom-left), going clockwise
  // SVG rotation: -90° (top) + 150° = 60°
  const rotateStart = 150 - 90; // = 60 degrees

  return (
    <svg
      width={size}
      height={size}
      viewBox={`0 0 ${size} ${size}`}
      className="overflow-visible"
      role="img"
      aria-label={`Reputation score: ${score}`}
    >
      <defs>
        <filter id="gauge-glow">
          <feGaussianBlur stdDeviation="3" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      {/* Track (background arc) */}
      <circle
        cx={cx}
        cy={cy}
        r={GAUGE_RADIUS}
        fill="none"
        stroke="rgba(148,163,184,0.12)"
        strokeWidth={STROKE_WIDTH}
        strokeDasharray={`${ARC_LENGTH} ${CIRCUMFERENCE - ARC_LENGTH}`}
        strokeDashoffset={0}
        strokeLinecap="round"
        transform={`rotate(${rotateStart} ${cx} ${cy})`}
      />

      {/* Filled arc */}
      <circle
        cx={cx}
        cy={cy}
        r={GAUGE_RADIUS}
        fill="none"
        stroke={color}
        strokeWidth={STROKE_WIDTH}
        strokeDasharray={`${filled} ${CIRCUMFERENCE - filled}`}
        strokeDashoffset={0}
        strokeLinecap="round"
        transform={`rotate(${rotateStart} ${cx} ${cy})`}
        filter="url(#gauge-glow)"
        style={{ transition: "stroke-dasharray 0.8s cubic-bezier(0.4,0,0.2,1), stroke 0.5s ease" }}
      />
    </svg>
  );
}

// ── Risk badge ────────────────────────────────────────────────────────────────

function RiskBadge({ level }: { level: string }) {
  const cfg: Record<string, { label: string; cls: string }> = {
    low: {
      label: "LOW RISK",
      cls: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
    },
    moderate: {
      label: "MODERATE",
      cls: "bg-yellow-500/15 text-yellow-400 border-yellow-500/30",
    },
    high: {
      label: "HIGH RISK",
      cls: "bg-red-500/15 text-red-400 border-red-500/30",
    },
  };
  const { label, cls } = cfg[level] ?? cfg.moderate;
  return (
    <span
      className={`inline-block px-3 py-1 rounded-full border text-xs font-bold uppercase tracking-widest ${cls}`}
    >
      {label}
    </span>
  );
}

// ── Mini stat ─────────────────────────────────────────────────────────────────

function MiniStat({
  label,
  value,
  color,
  icon,
}: {
  label: string;
  value: number;
  color: string;
  icon: string;
}) {
  return (
    <div className="flex flex-col items-center gap-1 min-w-0">
      <span className="text-xl">{icon}</span>
      <span
        className="text-lg font-bold tabular-nums leading-none"
        style={{ color }}
      >
        {value.toLocaleString()}
      </span>
      <span className="text-[11px] text-slate-500 uppercase tracking-wide">
        {label}
      </span>
    </div>
  );
}

// ── Skeleton ──────────────────────────────────────────────────────────────────

function ScoreCardSkeleton() {
  return (
    <div className="flex flex-col items-center gap-4 animate-pulse py-4">
      <div className="h-40 w-40 rounded-full bg-slate-700/50" />
      <div className="h-5 w-24 bg-slate-700/50 rounded-full" />
      <div className="flex gap-8">
        {[1, 2, 3].map((i) => (
          <div key={i} className="flex flex-col items-center gap-2">
            <div className="h-4 w-4 bg-slate-700/50 rounded" />
            <div className="h-5 w-8 bg-slate-700/50 rounded" />
            <div className="h-3 w-12 bg-slate-700/50 rounded" />
          </div>
        ))}
      </div>
    </div>
  );
}

// ── Error state ───────────────────────────────────────────────────────────────

function ErrorState() {
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-8 text-center">
      <span className="text-3xl">⚠️</span>
      <p className="text-red-400 text-sm font-medium">Failed to load score</p>
      <p className="text-slate-500 text-xs">Check your connection and retry</p>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

interface ReputationScoreCardProps {
  keyword: string;
}

export default function ReputationScoreCard({
  keyword,
}: ReputationScoreCardProps) {
  const { data, isLoading, isError } = useCurrentScore(keyword);

  const score = data as ReputationScore | undefined;

  const displayScore = score?.score ?? 0;
  const color = scoreToColor(displayScore);

  if (isLoading) return <ScoreCardSkeleton />;
  if (isError || !score) return <ErrorState />;

  const formattedScore =
    (displayScore > 0 ? "+" : "") + displayScore.toFixed(1);

  return (
    <div className="flex flex-col items-center gap-4">
      {/* Gauge + center text */}
      <div className="relative flex items-center justify-center">
        <CircularGauge score={displayScore} color={color} />

        {/* Center overlay */}
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span
            className="text-3xl font-black tabular-nums leading-none transition-all duration-700"
            style={{ color }}
          >
            {formattedScore}
          </span>
          <span className="text-slate-500 text-xs mt-1 uppercase tracking-widest">
            Score
          </span>
        </div>
      </div>

      {/* Risk badge */}
      <RiskBadge level={score.risk_level} />

      {/* Mini stats row */}
      <div className="flex items-center justify-center gap-8 pt-1">
        <MiniStat
          label="Positive"
          value={score.positive_count}
          color="#10B981"
          icon="📈"
        />
        <div className="h-10 w-px bg-slate-700/50" />
        <MiniStat
          label="Negative"
          value={score.negative_count}
          color="#EF4444"
          icon="📉"
        />
        <div className="h-10 w-px bg-slate-700/50" />
        <MiniStat
          label="Neutral"
          value={score.neutral_count}
          color="#3B82F6"
          icon="➖"
        />
      </div>

      {/* Total */}
      <p className="text-xs text-slate-500">
        Based on{" "}
        <span className="text-slate-300 font-semibold">
          {score.total_count.toLocaleString()}
        </span>{" "}
        posts · {(score.negative_ratio * 100).toFixed(1)}% negative ratio
      </p>
    </div>
  );
}
