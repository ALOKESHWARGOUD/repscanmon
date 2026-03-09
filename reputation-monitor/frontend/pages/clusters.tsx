import { useState } from "react";
import dynamic from "next/dynamic";
import Sidebar from "@/components/Sidebar";
import { useClusters, useKeywords } from "@/hooks/useKeywordData";

const AttackClusterGraph = dynamic(
  () => import("@/components/AttackClusterGraph"),
  { ssr: false, loading: () => <div className="h-64 flex items-center justify-center text-slate-500 text-sm">Loading graph…</div> }
);
import type { AttackCluster } from "@/lib/api";
import { formatDistanceToNow, parseISO } from "date-fns";

// ── Helpers ────────────────────────────────────────────────────────────────────

function timeAgo(iso: string): string {
  try {
    return formatDistanceToNow(parseISO(iso), { addSuffix: true });
  } catch {
    return "—";
  }
}

function confidenceColor(score: number): { bar: string; text: string } {
  if (score >= 0.7) return { bar: "bg-red-500", text: "text-red-400" };
  if (score >= 0.4) return { bar: "bg-yellow-500", text: "text-yellow-400" };
  return { bar: "bg-emerald-500", text: "text-emerald-400" };
}

function statusBadge(status: string): string {
  const map: Record<string, string> = {
    active: "bg-red-500/15 text-red-400 border-red-500/30",
    resolved: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
    investigating: "bg-yellow-500/15 text-yellow-400 border-yellow-500/30",
    dismissed: "bg-slate-700/50 text-slate-500 border-slate-600/30",
  };
  return map[status?.toLowerCase()] ?? map.investigating;
}

// ── Cluster Card ──────────────────────────────────────────────────────────────

function ClusterCard({ cluster }: { cluster: AttackCluster }) {
  const [expanded, setExpanded] = useState(false);
  const colors = confidenceColor(cluster.confidence_score);
  const confPct = Math.round(cluster.confidence_score * 100);
  const truncId = cluster.id.length > 12 ? cluster.id.slice(0, 12) + "…" : cluster.id;

  return (
    <div className="bg-slate-900 border border-slate-700/50 rounded-xl overflow-hidden hover:border-slate-600/70 transition-colors">
      <div className="p-5">
        {/* Row 1: ID + badges */}
        <div className="flex items-start justify-between gap-3 mb-3">
          <div className="flex items-center gap-2 flex-wrap min-w-0">
            <span className="font-mono text-xs text-slate-500 bg-slate-800 px-2 py-0.5 rounded border border-slate-700/50" title={cluster.id}>
              #{truncId}
            </span>
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-blue-600/15 border border-blue-500/20 text-blue-400 text-xs font-bold">
              👥 {cluster.cluster_size} members
            </span>
            <span className={`px-2 py-0.5 rounded border text-[11px] font-bold uppercase tracking-wide ${statusBadge(cluster.status)}`}>
              {cluster.status}
            </span>
          </div>
          <span className="text-xs text-slate-500 whitespace-nowrap flex-shrink-0">
            {timeAgo(cluster.detected_at)}
          </span>
        </div>

        {/* Description */}
        {cluster.description && (
          <p className="text-sm text-slate-400 mb-3 line-clamp-2">
            {cluster.description}
          </p>
        )}

        {/* Confidence bar */}
        <div className="mb-4">
          <div className="flex items-center justify-between mb-1.5">
            <span className="text-[11px] text-slate-500 uppercase tracking-wider font-semibold">
              Confidence
            </span>
            <span className={`text-xs font-bold tabular-nums ${colors.text}`}>
              {confPct}%
            </span>
          </div>
          <div className="h-1.5 rounded-full bg-slate-700">
            <div
              className={`h-1.5 rounded-full transition-all duration-700 ${colors.bar}`}
              style={{ width: `${confPct}%` }}
            />
          </div>
        </div>

        {/* View members toggle */}
        <button
          onClick={() => setExpanded((v) => !v)}
          className="text-xs text-slate-400 hover:text-white transition-colors flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-slate-800 border border-slate-600/50 hover:border-slate-500"
        >
          {expanded ? "▲ Hide Members" : "▼ View Members"}
        </button>
      </div>

      {/* Expanded member list */}
      {expanded && (
        <div className="border-t border-slate-700/50 bg-slate-950/40 px-5 py-4">
          {cluster.member_ids && cluster.member_ids.length > 0 ? (
            <div className="flex flex-wrap gap-2">
              {cluster.member_ids.map((mid) => (
                <span
                  key={mid}
                  className="font-mono text-[11px] bg-slate-800 border border-slate-700/50 text-slate-400 px-2 py-0.5 rounded"
                  title={mid}
                >
                  {mid.length > 16 ? mid.slice(0, 16) + "…" : mid}
                </span>
              ))}
            </div>
          ) : (
            <p className="text-xs text-slate-600">No member IDs available.</p>
          )}
        </div>
      )}
    </div>
  );
}

// ── Skeleton ──────────────────────────────────────────────────────────────────

function ClusterSkeleton() {
  return (
    <div className="bg-slate-900 border border-slate-700/50 rounded-xl p-5 animate-pulse space-y-3">
      <div className="flex items-center gap-2">
        <div className="h-5 w-24 bg-slate-700/50 rounded" />
        <div className="h-5 w-16 bg-slate-700/50 rounded" />
        <div className="h-5 w-16 bg-slate-700/50 rounded" />
      </div>
      <div className="h-4 w-3/4 bg-slate-700/50 rounded" />
      <div className="h-2 w-full bg-slate-700/50 rounded-full" />
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function Clusters() {
  const [selectedKeyword, setSelectedKeyword] = useState("");
  const [page, setPage] = useState(1);

  const { data: keywordsData } = useKeywords(1, 50);
  const keywords = keywordsData?.items ?? [];
  const activeKeyword = selectedKeyword || keywords[0]?.keyword || "";

  const { data: clustersData, isLoading, isError } = useClusters(activeKeyword, page);
  const clusters: AttackCluster[] = clustersData?.items ?? [];
  const total = clustersData?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / 10));

  return (
    <div className="flex min-h-screen bg-slate-950">
      <Sidebar />

      <div className="flex-1 ml-64 flex flex-col min-h-screen">
        {/* Header */}
        <header className="sticky top-0 z-30 flex h-16 items-center justify-between gap-4 border-b border-slate-700/50 bg-slate-950/90 backdrop-blur px-8">
          <div className="flex items-center gap-3">
            <h1 className="text-xl font-bold text-white">Coordinated Attack Detection</h1>
            <span className="rounded bg-red-600/20 border border-red-500/30 px-2 py-0.5 text-[11px] font-bold text-red-400 uppercase tracking-wider">
              🕸️ Clusters
            </span>
          </div>
          {/* Keyword selector */}
          <select
            value={activeKeyword}
            onChange={(e) => { setSelectedKeyword(e.target.value); setPage(1); }}
            className="rounded-lg bg-slate-800 border border-slate-600/50 px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500/70 focus:ring-1 focus:ring-blue-500/30 transition-colors min-w-[160px]"
          >
            {keywords.length === 0 && <option value="">No keywords</option>}
            {keywords.map((kw) => (
              <option key={kw.id} value={kw.keyword}>{kw.keyword}</option>
            ))}
          </select>
        </header>

        <main className="flex-1 p-8 space-y-8">
          {/* Network graph */}
          {activeKeyword && (
            <div className="bg-slate-900 border border-slate-700/50 rounded-xl overflow-hidden">
              <div className="flex items-center gap-2 px-5 py-3.5 border-b border-slate-700/50">
                <span>🕸️</span>
                <h2 className="text-sm font-bold text-white">Network Graph</h2>
                <span className="ml-2 text-xs text-slate-500">— {activeKeyword}</span>
              </div>
              <div className="p-5">
                <AttackClusterGraph keyword={activeKeyword} />
              </div>
            </div>
          )}

          {/* Cluster cards list */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-sm font-bold text-white flex items-center gap-2">
                🔍 Detected Clusters
                <span className="px-1.5 py-0.5 rounded bg-slate-700 text-slate-400 text-[11px] font-bold">
                  {total}
                </span>
              </h2>
              {totalPages > 1 && (
                <div className="flex items-center gap-2 text-xs text-slate-400">
                  <button
                    disabled={page <= 1}
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                    className="px-3 py-1.5 rounded bg-slate-800 border border-slate-600/50 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    ← Prev
                  </button>
                  <span>Page {page} / {totalPages}</span>
                  <button
                    disabled={page >= totalPages}
                    onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                    className="px-3 py-1.5 rounded bg-slate-800 border border-slate-600/50 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    Next →
                  </button>
                </div>
              )}
            </div>

            {isError && (
              <div className="text-center py-12 text-red-400 text-sm">
                ⚠️ Failed to load clusters. Check API connection.
              </div>
            )}

            {isLoading && (
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                {Array.from({ length: 6 }).map((_, i) => <ClusterSkeleton key={i} />)}
              </div>
            )}

            {!isLoading && !isError && clusters.length === 0 && (
              <div className="text-center py-16">
                <span className="text-5xl block mb-4">🕸️</span>
                <p className="text-slate-300 font-semibold">
                  {activeKeyword ? "No clusters detected" : "Select a keyword to view clusters"}
                </p>
                <p className="text-slate-500 text-sm mt-1">
                  {activeKeyword && "Coordinated attacks will appear here when detected"}
                </p>
              </div>
            )}

            {!isLoading && !isError && clusters.length > 0 && (
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                {clusters.map((cluster) => (
                  <ClusterCard key={cluster.id} cluster={cluster} />
                ))}
              </div>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}
