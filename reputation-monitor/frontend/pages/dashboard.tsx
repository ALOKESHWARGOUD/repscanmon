import { useState } from "react";
import { useRouter } from "next/router";
import Link from "next/link";
import Sidebar from "@/components/Sidebar";
import AlertsPanel from "@/components/AlertsPanel";
import {
  useKeywords,
  useCreateKeyword,
  useDeleteKeyword,
  useAlerts,
  useCurrentScore,
} from "@/hooks/useKeywordData";
import type { Keyword } from "@/lib/api";

// ── Helpers ────────────────────────────────────────────────────────────────────

function riskColor(score: number): { bar: string; text: string; badge: string } {
  if (score > 20) return { bar: "bg-emerald-500", text: "text-emerald-400", badge: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30" };
  if (score >= -20) return { bar: "bg-yellow-500", text: "text-yellow-400", badge: "bg-yellow-500/15 text-yellow-400 border-yellow-500/30" };
  return { bar: "bg-red-500", text: "text-red-400", badge: "bg-red-500/15 text-red-400 border-red-500/30" };
}

function riskLabel(score: number): string {
  if (score > 20) return "Low";
  if (score >= -20) return "Moderate";
  return "High";
}

function scoreBarWidth(score: number): string {
  const pct = Math.round(((score + 100) / 200) * 100);
  return `${Math.min(100, Math.max(0, pct))}%`;
}

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  } catch {
    return "—";
  }
}

// ── KPI Card ──────────────────────────────────────────────────────────────────

interface KpiCardProps {
  icon: string;
  title: string;
  value: string | number;
  sub?: string;
  trend?: "up" | "down" | "neutral";
  color?: string;
}

function KpiCard({ icon, title, value, sub, trend, color = "text-white" }: KpiCardProps) {
  const trendIcon = trend === "up" ? "↑" : trend === "down" ? "↓" : "—";
  const trendCls = trend === "up" ? "text-emerald-400" : trend === "down" ? "text-red-400" : "text-slate-500";
  return (
    <div className="bg-slate-800 border border-slate-700/50 rounded-xl p-5 flex flex-col gap-3 hover:border-slate-600/70 transition-colors">
      <div className="flex items-center justify-between">
        <span className="text-2xl">{icon}</span>
        <span className={`text-sm font-bold ${trendCls}`}>{trendIcon}</span>
      </div>
      <div>
        <p className="text-slate-400 text-xs font-medium uppercase tracking-wider mb-1">{title}</p>
        <p className={`text-3xl font-black tabular-nums leading-none ${color}`}>{value}</p>
      </div>
      {sub && <p className="text-xs text-slate-500">{sub}</p>}
    </div>
  );
}

// ── Keyword Score Cell (fetches score per keyword) ────────────────────────────

function KeywordScoreCell({ keyword }: { keyword: string }) {
  const { data } = useCurrentScore(keyword);
  const score = data?.score ?? 0;
  const colors = riskColor(score);
  const pct = scoreBarWidth(score);
  const label = (score > 0 ? "+" : "") + score.toFixed(1);

  return (
    <div className="flex items-center gap-2 min-w-[120px]">
      <div className="flex-1 bg-slate-700 rounded-full h-1.5">
        <div
          className={`h-1.5 rounded-full transition-all duration-700 ${colors.bar}`}
          style={{ width: pct }}
        />
      </div>
      <span className={`text-xs font-bold tabular-nums w-12 text-right ${colors.text}`}>
        {label}
      </span>
    </div>
  );
}

function KeywordRiskCell({ keyword }: { keyword: string }) {
  const { data } = useCurrentScore(keyword);
  const score = data?.score ?? 0;
  const colors = riskColor(score);
  return (
    <span className={`inline-block px-2 py-0.5 rounded border text-[11px] font-bold uppercase tracking-wide ${colors.badge}`}>
      {riskLabel(score)}
    </span>
  );
}

// ── Platform icons ─────────────────────────────────────────────────────────────

const PLATFORM_ICONS: Record<string, string> = {
  twitter: "𝕏",
  reddit: "🔴",
  youtube: "🎬",
  news: "📰",
};

// ── Dashboard stat aggregator ──────────────────────────────────────────────────

function DashboardKpiRow({ keywords }: { keywords: Keyword[] }) {
  const alertsData = useAlerts(1, 100);
  const alerts = alertsData.data?.items ?? [];
  const unreadAlerts = alerts.filter((a) => !a.is_read).length;
  const totalKeywords = keywords.length;

  return (
    <div className="grid grid-cols-2 xl:grid-cols-4 gap-4 mb-8">
      <KpiCard
        icon="🔑"
        title="Total Keywords"
        value={totalKeywords}
        sub="Active monitoring targets"
        trend="neutral"
      />
      <KpiCard
        icon="🔔"
        title="Active Alerts"
        value={unreadAlerts}
        sub={`${alerts.length} total alerts`}
        trend={unreadAlerts > 0 ? "down" : "neutral"}
        color={unreadAlerts > 0 ? "text-red-400" : "text-white"}
      />
      <KpiCard
        icon="⚠️"
        title="Flagged Authors"
        value={0}
        sub="Coordinated attack actors"
        trend="neutral"
      />
      <KpiCard
        icon="🕸️"
        title="Attack Clusters"
        value={0}
        sub="Detected this week"
        trend="neutral"
      />
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────

export default function Dashboard() {
  const router = useRouter();
  const [newKeyword, setNewKeyword] = useState("");
  const [addError, setAddError] = useState("");

  const { data: keywordsData, isLoading } = useKeywords(1, 50);
  const createKeyword = useCreateKeyword();
  const deleteKeyword = useDeleteKeyword();

  const keywords: Keyword[] = keywordsData?.items ?? [];

  async function handleAdd(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = newKeyword.trim();
    if (!trimmed) return;
    setAddError("");
    try {
      await createKeyword.mutateAsync(trimmed);
      setNewKeyword("");
      router.push(`/keyword/${encodeURIComponent(trimmed)}`);
    } catch {
      setAddError("Failed to add keyword. Check API connection.");
    }
  }

  async function handleDelete(id: string) {
    await deleteKeyword.mutateAsync(id);
  }

  return (
    <div className="flex min-h-screen bg-slate-950">
      <Sidebar />

      {/* Main content */}
      <div className="flex-1 ml-64 flex flex-col min-h-screen">
        {/* Top header */}
        <header className="sticky top-0 z-30 flex h-16 items-center justify-between gap-4 border-b border-slate-700/50 bg-slate-950/90 backdrop-blur px-8">
          <div className="flex items-center gap-3">
            <h1 className="text-xl font-bold text-white">Dashboard</h1>
            <span className="rounded bg-blue-600/20 border border-blue-500/30 px-2 py-0.5 text-[11px] font-bold text-blue-400 uppercase tracking-wider">
              Live
            </span>
          </div>
          <div className="flex items-center gap-3">
            <div className="h-8 w-8 rounded-full bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center text-white text-xs font-bold select-none">
              U
            </div>
          </div>
        </header>

        <main className="flex-1 p-8">
          {/* KPI Row */}
          {!isLoading && <DashboardKpiRow keywords={keywords} />}

          <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
            {/* Keywords table — takes 2/3 */}
            <div className="xl:col-span-2">
              <div className="bg-slate-900 border border-slate-700/50 rounded-xl overflow-hidden">
                {/* Table header */}
                <div className="flex items-center justify-between px-6 py-4 border-b border-slate-700/50">
                  <h2 className="text-sm font-bold text-white flex items-center gap-2">
                    🔑 <span>Monitored Keywords</span>
                    <span className="ml-1 px-1.5 py-0.5 rounded bg-slate-700 text-slate-400 text-[11px] font-bold">
                      {keywords.length}
                    </span>
                  </h2>
                </div>

                {/* Add keyword form */}
                <form onSubmit={handleAdd} className="flex items-center gap-3 px-6 py-3 border-b border-slate-700/30 bg-slate-800/30">
                  <input
                    type="text"
                    placeholder="Add keyword to monitor…"
                    value={newKeyword}
                    onChange={(e) => setNewKeyword(e.target.value)}
                    className="flex-1 rounded-lg bg-slate-800 border border-slate-600/50 px-4 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500/70 focus:ring-1 focus:ring-blue-500/30 transition-colors"
                  />
                  <button
                    type="submit"
                    disabled={createKeyword.isPending || !newKeyword.trim()}
                    className="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-semibold transition-colors"
                  >
                    {createKeyword.isPending ? "Adding…" : "+ Add"}
                  </button>
                </form>
                {addError && (
                  <p className="px-6 py-2 text-xs text-red-400 bg-red-500/10 border-b border-red-500/20">
                    {addError}
                  </p>
                )}

                {/* Table */}
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-slate-700/50">
                        {["Keyword", "Score", "Risk", "Platforms", "Status", "Created", "Actions"].map((h) => (
                          <th
                            key={h}
                            className="px-4 py-3 text-left text-[11px] font-bold text-slate-500 uppercase tracking-wider whitespace-nowrap"
                          >
                            {h}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-700/30">
                      {isLoading &&
                        Array.from({ length: 4 }).map((_, i) => (
                          <tr key={i} className="animate-pulse">
                            {Array.from({ length: 7 }).map((__, j) => (
                              <td key={j} className="px-4 py-3">
                                <div className="h-4 rounded bg-slate-700/50" />
                              </td>
                            ))}
                          </tr>
                        ))}

                      {!isLoading && keywords.length === 0 && (
                        <tr>
                          <td colSpan={7} className="px-4 py-10 text-center text-slate-500 text-sm">
                            No keywords yet. Add one above to start monitoring.
                          </td>
                        </tr>
                      )}

                      {keywords.map((kw) => (
                        <tr
                          key={kw.id}
                          className="hover:bg-slate-800/40 transition-colors group"
                        >
                          {/* Keyword */}
                          <td className="px-4 py-3">
                            <span className="font-semibold text-white">{kw.keyword}</span>
                          </td>

                          {/* Score bar */}
                          <td className="px-4 py-3">
                            <KeywordScoreCell keyword={kw.keyword} />
                          </td>

                          {/* Risk badge */}
                          <td className="px-4 py-3">
                            <KeywordRiskCell keyword={kw.keyword} />
                          </td>

                          {/* Platforms */}
                          <td className="px-4 py-3">
                            <div className="flex gap-1.5">
                              {Object.entries(PLATFORM_ICONS).map(([p, icon]) => (
                                <span
                                  key={p}
                                  title={p}
                                  className="text-sm opacity-70 hover:opacity-100 transition-opacity"
                                >
                                  {icon}
                                </span>
                              ))}
                            </div>
                          </td>

                          {/* Status */}
                          <td className="px-4 py-3">
                            <span
                              className={`inline-block px-2 py-0.5 rounded border text-[11px] font-bold uppercase tracking-wide ${
                                kw.is_active
                                  ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/20"
                                  : "bg-slate-700/50 text-slate-500 border-slate-600/20"
                              }`}
                            >
                              {kw.is_active ? "Active" : "Paused"}
                            </span>
                          </td>

                          {/* Created */}
                          <td className="px-4 py-3 text-slate-500 whitespace-nowrap text-xs">
                            {formatDate(kw.created_at)}
                          </td>

                          {/* Actions */}
                          <td className="px-4 py-3">
                            <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                              <Link
                                href={`/keyword/${encodeURIComponent(kw.keyword)}`}
                                className="px-3 py-1 rounded bg-blue-600/20 border border-blue-500/30 text-blue-400 text-xs font-semibold hover:bg-blue-600/40 transition-colors whitespace-nowrap"
                              >
                                View Live →
                              </Link>
                              <button
                                onClick={() => handleDelete(kw.id)}
                                className="px-2 py-1 rounded bg-red-500/10 border border-red-500/20 text-red-400 text-xs font-semibold hover:bg-red-500/20 transition-colors"
                                title="Delete keyword"
                              >
                                🗑
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            {/* Right panel — Recent alerts */}
            <div className="xl:col-span-1">
              <div className="bg-slate-900 border border-slate-700/50 rounded-xl p-5">
                <AlertsPanel limit={5} />
              </div>
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
