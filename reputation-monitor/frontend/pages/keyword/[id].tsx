import { useRouter } from "next/router";
import dynamic from "next/dynamic";
import Link from "next/link";
import Sidebar from "@/components/Sidebar";
import SentimentTimelineChart from "@/components/SentimentTimelineChart";
import PlatformBreakdownChart from "@/components/PlatformBreakdownChart";
import type { PlatformBreakdown } from "@/components/PlatformBreakdownChart";
import TopNegativePostsList from "@/components/TopNegativePostsList";
import ReputationScoreCard from "@/components/ReputationScoreCard";
import LiveFeedPanel from "@/components/LiveFeedPanel";
import {
  useSentimentSummary,
  useTimeline,
  useCurrentScore,
} from "@/hooks/useKeywordData";

const AttackClusterGraph = dynamic(
  () => import("@/components/AttackClusterGraph"),
  { ssr: false, loading: () => <div className="h-48 flex items-center justify-center text-slate-500 text-sm">Loading graph…</div> }
);

// ── Helpers ────────────────────────────────────────────────────────────────────

function riskBadge(level: string) {
  const map: Record<string, string> = {
    low: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
    moderate: "bg-yellow-500/15 text-yellow-400 border-yellow-500/30",
    high: "bg-red-500/15 text-red-400 border-red-500/30",
  };
  return map[level] ?? map.moderate;
}

function riskLabel(level: string): string {
  const map: Record<string, string> = {
    low: "LOW RISK",
    moderate: "MODERATE",
    high: "HIGH RISK",
  };
  return map[level] ?? "UNKNOWN";
}

// ── Skeleton ──────────────────────────────────────────────────────────────────

function SectionSkeleton({ rows = 4, height = "h-4" }: { rows?: number; height?: string }) {
  return (
    <div className="animate-pulse space-y-3">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className={`${height} rounded bg-slate-700/50`} />
      ))}
    </div>
  );
}

// ── Section wrapper ────────────────────────────────────────────────────────────

function Section({ title, icon, children, className = "" }: {
  title: string;
  icon: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={`bg-slate-900 border border-slate-700/50 rounded-xl overflow-hidden ${className}`}>
      <div className="flex items-center gap-2 px-5 py-3.5 border-b border-slate-700/50">
        <span className="text-base">{icon}</span>
        <h2 className="text-sm font-bold text-white">{title}</h2>
      </div>
      <div className="p-5">{children}</div>
    </div>
  );
}

// ── Animated score header ──────────────────────────────────────────────────────

function KeywordHeader({ keyword }: { keyword: string }) {
  const { data: score } = useCurrentScore(keyword);
  const { data: sentiment } = useSentimentSummary(keyword);

  const riskLevel = score?.risk_level ?? sentiment?.risk_level ?? "moderate";
  const scoreVal = score?.score ?? 0;
  const scoreStr = (scoreVal > 0 ? "+" : "") + scoreVal.toFixed(1);

  const scoreColor =
    scoreVal > 20 ? "text-emerald-400" : scoreVal >= -20 ? "text-yellow-400" : "text-red-400";

  return (
    <div className="flex flex-wrap items-center gap-4">
      <div className="flex items-center gap-3">
        <div className="h-10 w-10 rounded-lg bg-blue-600/20 border border-blue-500/30 flex items-center justify-center">
          <span className="text-blue-400 font-bold text-sm">🔑</span>
        </div>
        <div>
          <h1 className="text-2xl font-black text-white tracking-tight">{keyword}</h1>
          <p className="text-slate-500 text-xs mt-0.5">Reputation Intelligence · Live</p>
        </div>
      </div>

      <div className="flex items-center gap-3 ml-auto flex-wrap">
        {score && (
          <div className="flex items-center gap-2 px-4 py-2 rounded-lg bg-slate-800 border border-slate-700/50">
            <span className="text-slate-400 text-xs">Score</span>
            <span className={`text-xl font-black tabular-nums ${scoreColor}`}>{scoreStr}</span>
          </div>
        )}
        <span className={`px-3 py-1.5 rounded-lg border text-xs font-bold uppercase tracking-widest ${riskBadge(riskLevel)}`}>
          {riskLabel(riskLevel)}
        </span>
        <Link
          href="/dashboard"
          className="px-3 py-1.5 rounded-lg bg-slate-800 border border-slate-600/50 text-slate-400 hover:text-white text-xs font-semibold transition-colors"
        >
          ← Back
        </Link>
      </div>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function KeywordDetail() {
  const router = useRouter();
  const id = typeof router.query.id === "string" ? router.query.id : "";

  const decodedKeyword = id ? decodeURIComponent(id) : "";

  const { data: timelineData, isLoading: timelineLoading } = useTimeline(decodedKeyword);
  const { data: sentimentData, isLoading: sentimentLoading } = useSentimentSummary(decodedKeyword);

  // Build platform breakdown from sentiment summary
  const platformBreakdown: PlatformBreakdown[] = sentimentData
    ? [
        {
          platform: "total",
          positive: sentimentData.positive_count,
          negative: sentimentData.negative_count,
          neutral: sentimentData.neutral_count,
          total: sentimentData.total_count,
        },
      ]
    : [];

  if (!decodedKeyword) {
    return (
      <div className="flex min-h-screen bg-slate-950 items-center justify-center">
        <div className="text-center text-slate-500">
          <p className="text-4xl mb-3">🔍</p>
          <p className="text-lg font-semibold text-white">No keyword specified</p>
          <Link href="/dashboard" className="mt-4 inline-block text-blue-400 hover:text-blue-300 text-sm">
            ← Return to Dashboard
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen bg-slate-950">
      <Sidebar />

      <div className="flex-1 ml-64 flex flex-col min-h-screen">
        {/* Header */}
        <header className="sticky top-0 z-30 border-b border-slate-700/50 bg-slate-950/90 backdrop-blur px-8 py-4">
          <KeywordHeader keyword={decodedKeyword} />
        </header>

        <main className="flex-1 p-8">
          {/* 12-col grid: left 7, right 5 */}
          <div className="grid grid-cols-12 gap-6">
            {/* Left column — 7 cols */}
            <div className="col-span-12 xl:col-span-7 flex flex-col gap-6">
              {/* Sentiment Timeline */}
              <Section title="Sentiment Timeline" icon="📈">
                {timelineLoading ? (
                  <SectionSkeleton rows={6} height="h-6" />
                ) : (
                  <SentimentTimelineChart data={timelineData ?? []} />
                )}
              </Section>

              {/* Platform Breakdown */}
              <Section title="Platform Breakdown" icon="🌐">
                {sentimentLoading ? (
                  <SectionSkeleton rows={4} height="h-8" />
                ) : (
                  <PlatformBreakdownChart data={platformBreakdown} />
                )}
              </Section>

              {/* Top Negative Posts */}
              <Section title="Top Negative Posts" icon="📉">
                <TopNegativePostsList keyword={decodedKeyword} />
              </Section>

              {/* Attack Cluster Graph */}
              <Section title="Attack Cluster Graph" icon="🕸️">
                <AttackClusterGraph keyword={decodedKeyword} />
              </Section>
            </div>

            {/* Right column — 5 cols */}
            <div className="col-span-12 xl:col-span-5 flex flex-col gap-6">
              {/* Reputation Score Card */}
              <Section title="Reputation Score" icon="🎯">
                <ReputationScoreCard keyword={decodedKeyword} />
              </Section>

              {/* Live Feed Panel */}
              <div className="bg-slate-900 border border-slate-700/50 rounded-xl overflow-hidden">
                <div className="flex items-center gap-2 px-5 py-3.5 border-b border-slate-700/50">
                  <span className="text-base">📡</span>
                  <h2 className="text-sm font-bold text-white">Live Feed</h2>
                  <span className="ml-auto flex h-2 w-2">
                    <span className="animate-ping absolute inline-flex h-2 w-2 rounded-full bg-red-400 opacity-75" />
                    <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500" />
                  </span>
                </div>
                <div className="overflow-y-auto" style={{ maxHeight: "600px" }}>
                  <div className="p-5">
                    <LiveFeedPanel keyword={decodedKeyword} />
                  </div>
                </div>
              </div>
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
