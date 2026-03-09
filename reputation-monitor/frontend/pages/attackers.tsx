import { useState, useMemo } from "react";
import Sidebar from "@/components/Sidebar";
import { useAttackers, useKeywords } from "@/hooks/useKeywordData";
import type { TrackedAuthor } from "@/lib/api";
import { formatDistanceToNow, parseISO } from "date-fns";

// ── Helpers ────────────────────────────────────────────────────────────────────

const PLATFORM_ICONS: Record<string, string> = {
  twitter: "𝕏",
  instagram: "📸",
  youtube: "▶️",
};

function platformIcon(p: string): string {
  return PLATFORM_ICONS[p?.toLowerCase()] ?? "🌐";
}

function formatFollowers(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(1) + "K";
  return String(n);
}

function timeAgo(iso: string): string {
  try {
    return formatDistanceToNow(parseISO(iso), { addSuffix: true });
  } catch {
    return "—";
  }
}

function riskBarColor(score: number): string {
  if (score < 30) return "bg-emerald-500";
  if (score < 60) return "bg-yellow-500";
  return "bg-red-500";
}

function riskTextColor(score: number): string {
  if (score < 30) return "text-emerald-400";
  if (score < 60) return "text-yellow-400";
  return "text-red-400";
}

// ── Types ──────────────────────────────────────────────────────────────────────

type SortKey = keyof Pick<TrackedAuthor, "risk_score" | "negative_post_count" | "followers_count">;

// ── Risk Score Cell ────────────────────────────────────────────────────────────

function RiskScoreBar({ score }: { score: number }) {
  const pct = Math.min(100, Math.max(0, score));
  return (
    <div className="flex items-center gap-2 min-w-[100px]">
      <div className="flex-1 bg-slate-700 rounded-full h-1.5">
        <div
          className={`h-1.5 rounded-full transition-all duration-500 ${riskBarColor(pct)}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className={`text-xs font-bold tabular-nums w-8 text-right ${riskTextColor(pct)}`}>
        {pct.toFixed(0)}%
      </span>
    </div>
  );
}

// ── Attacker Row ───────────────────────────────────────────────────────────────

function AttackerRow({ author }: { author: TrackedAuthor }) {
  return (
    <tr className="hover:bg-slate-800/40 transition-colors group">
      {/* Author */}
      <td className="px-4 py-3">
        <div className="flex items-center gap-2">
          <div className="h-8 w-8 rounded-full bg-slate-700 flex items-center justify-center text-slate-400 text-sm font-bold select-none flex-shrink-0">
            {author.author_name?.[0]?.toUpperCase() ?? "?"}
          </div>
          <div className="min-w-0">
            <p className="text-sm font-semibold text-white truncate max-w-[140px]">
              {author.author_name}
            </p>
            <p className="text-xs text-slate-500 truncate">
              @{author.author_id}
            </p>
          </div>
        </div>
      </td>

      {/* Platform */}
      <td className="px-4 py-3">
        <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-slate-700/60 border border-slate-600/40 text-xs text-slate-300 font-medium">
          <span>{platformIcon(author.platform)}</span>
          <span className="capitalize">{author.platform}</span>
        </span>
      </td>

      {/* Risk Score */}
      <td className="px-4 py-3">
        <RiskScoreBar score={author.risk_score * 100} />
      </td>

      {/* Negative Posts */}
      <td className="px-4 py-3">
        <span className="inline-block px-2 py-0.5 rounded bg-red-500/15 border border-red-500/20 text-red-400 text-xs font-bold">
          {author.negative_post_count}
        </span>
      </td>

      {/* Followers */}
      <td className="px-4 py-3 text-slate-300 text-sm tabular-nums">
        {formatFollowers(author.followers_count)}
      </td>

      {/* Account Age */}
      <td className="px-4 py-3 text-slate-500 text-xs">
        N/A
      </td>

      {/* Last Seen */}
      <td className="px-4 py-3 text-slate-500 text-xs whitespace-nowrap">
        {timeAgo(author.last_seen_at)}
      </td>

      {/* Actions */}
      <td className="px-4 py-3">
        <div className="opacity-0 group-hover:opacity-100 transition-opacity">
          <a
            href={`https://twitter.com/${author.author_id}`}
            target="_blank"
            rel="noopener noreferrer"
            className="px-3 py-1 rounded bg-blue-600/20 border border-blue-500/30 text-blue-400 text-xs font-semibold hover:bg-blue-600/40 transition-colors whitespace-nowrap"
          >
            View Profile →
          </a>
        </div>
      </td>
    </tr>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function Attackers() {
  const [selectedKeyword, setSelectedKeyword] = useState("");
  const [platformFilter, setPlatformFilter] = useState("all");
  const [riskFilter, setRiskFilter] = useState("all");
  const [flaggedOnly, setFlaggedOnly] = useState(false);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("risk_score");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const { data: keywordsData } = useKeywords(1, 50);
  const keywords = keywordsData?.items ?? [];

  const activeKeyword = selectedKeyword || keywords[0]?.keyword || "";

  const { data: attackersData, isLoading, isError } = useAttackers(activeKeyword, {
    platform: platformFilter === "all" ? undefined : platformFilter,
    flagged_only: flaggedOnly || undefined,
  });

  const authors: TrackedAuthor[] = attackersData?.items ?? [];

  const filtered = useMemo(() => {
    let list = [...authors];

    if (search) {
      const q = search.toLowerCase();
      list = list.filter(
        (a) =>
          a.author_name?.toLowerCase().includes(q) ||
          a.author_id?.toLowerCase().includes(q)
      );
    }

    if (riskFilter !== "all") {
      list = list.filter((a) => {
        const pct = a.risk_score * 100;
        if (riskFilter === "high") return pct >= 60;
        if (riskFilter === "moderate") return pct >= 30 && pct < 60;
        if (riskFilter === "low") return pct < 30;
        return true;
      });
    }

    list.sort((a, b) => {
      const av = a[sortKey] as number;
      const bv = b[sortKey] as number;
      return sortDir === "desc" ? bv - av : av - bv;
    });

    return list;
  }, [authors, search, riskFilter, sortKey, sortDir]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  }

  function SortIcon({ col }: { col: SortKey }) {
    if (sortKey !== col) return <span className="text-slate-600 ml-1">⇅</span>;
    return <span className="text-blue-400 ml-1">{sortDir === "desc" ? "↓" : "↑"}</span>;
  }

  const flaggedCount = authors.filter((a) => a.is_flagged).length;

  return (
    <div className="flex min-h-screen bg-slate-950">
      <Sidebar />

      <div className="flex-1 ml-64 flex flex-col min-h-screen">
        {/* Header */}
        <header className="sticky top-0 z-30 flex h-16 items-center justify-between gap-4 border-b border-slate-700/50 bg-slate-950/90 backdrop-blur px-8">
          <div className="flex items-center gap-3">
            <h1 className="text-xl font-bold text-white">Flagged Authors & Attack Vectors</h1>
          </div>
          <div className="flex items-center gap-2 text-sm text-slate-400">
            <span>{filtered.length} authors</span>
            {flaggedCount > 0 && (
              <span className="px-2 py-0.5 rounded bg-red-500/15 border border-red-500/20 text-red-400 text-xs font-bold">
                {flaggedCount} flagged
              </span>
            )}
          </div>
        </header>

        <main className="flex-1 p-8">
          {/* Filters bar */}
          <div className="flex flex-wrap items-center gap-3 mb-6 p-4 bg-slate-900 border border-slate-700/50 rounded-xl">
            {/* Keyword selector */}
            <select
              value={activeKeyword}
              onChange={(e) => setSelectedKeyword(e.target.value)}
              className="rounded-lg bg-slate-800 border border-slate-600/50 px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500/70 focus:ring-1 focus:ring-blue-500/30 transition-colors min-w-[160px]"
            >
              {keywords.length === 0 && (
                <option value="">No keywords</option>
              )}
              {keywords.map((kw) => (
                <option key={kw.id} value={kw.keyword}>
                  {kw.keyword}
                </option>
              ))}
            </select>

            {/* Platform filter */}
            <select
              value={platformFilter}
              onChange={(e) => setPlatformFilter(e.target.value)}
              className="rounded-lg bg-slate-800 border border-slate-600/50 px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500/70 focus:ring-1 focus:ring-blue-500/30 transition-colors"
            >
              <option value="all">All Platforms</option>
              <option value="twitter">𝕏 Twitter</option>
              <option value="instagram">📸 Instagram</option>
              <option value="youtube">▶️ YouTube</option>
            </select>

            {/* Risk filter */}
            <select
              value={riskFilter}
              onChange={(e) => setRiskFilter(e.target.value)}
              className="rounded-lg bg-slate-800 border border-slate-600/50 px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500/70 focus:ring-1 focus:ring-blue-500/30 transition-colors"
            >
              <option value="all">All Risk Levels</option>
              <option value="high">High Risk (≥60%)</option>
              <option value="moderate">Moderate (30–60%)</option>
              <option value="low">Low (&lt;30%)</option>
            </select>

            {/* Flagged only */}
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={flaggedOnly}
                onChange={(e) => setFlaggedOnly(e.target.checked)}
                className="w-4 h-4 rounded border-slate-600 bg-slate-800 text-blue-600 focus:ring-blue-500/30 focus:ring-1"
              />
              <span className="text-sm text-slate-400">Flagged only</span>
            </label>

            {/* Search */}
            <div className="flex-1 min-w-[200px]">
              <input
                type="text"
                placeholder="Search authors…"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="w-full rounded-lg bg-slate-800 border border-slate-600/50 px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500/70 focus:ring-1 focus:ring-blue-500/30 transition-colors"
              />
            </div>
          </div>

          {/* Table */}
          <div className="bg-slate-900 border border-slate-700/50 rounded-xl overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-700/50">
                    <th className="px-4 py-3 text-left text-[11px] font-bold text-slate-500 uppercase tracking-wider">
                      Author
                    </th>
                    <th className="px-4 py-3 text-left text-[11px] font-bold text-slate-500 uppercase tracking-wider">
                      Platform
                    </th>
                    <th
                      className="px-4 py-3 text-left text-[11px] font-bold text-slate-500 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors select-none"
                      onClick={() => toggleSort("risk_score")}
                    >
                      Risk Score <SortIcon col="risk_score" />
                    </th>
                    <th
                      className="px-4 py-3 text-left text-[11px] font-bold text-slate-500 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors select-none"
                      onClick={() => toggleSort("negative_post_count")}
                    >
                      Neg. Posts <SortIcon col="negative_post_count" />
                    </th>
                    <th
                      className="px-4 py-3 text-left text-[11px] font-bold text-slate-500 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors select-none"
                      onClick={() => toggleSort("followers_count")}
                    >
                      Followers <SortIcon col="followers_count" />
                    </th>
                    <th className="px-4 py-3 text-left text-[11px] font-bold text-slate-500 uppercase tracking-wider">
                      Account Age
                    </th>
                    <th className="px-4 py-3 text-left text-[11px] font-bold text-slate-500 uppercase tracking-wider">
                      Last Seen
                    </th>
                    <th className="px-4 py-3 text-left text-[11px] font-bold text-slate-500 uppercase tracking-wider">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-700/30">
                  {isLoading &&
                    Array.from({ length: 5 }).map((_, i) => (
                      <tr key={i} className="animate-pulse">
                        {Array.from({ length: 8 }).map((__, j) => (
                          <td key={j} className="px-4 py-3">
                            <div className="h-4 rounded bg-slate-700/50" />
                          </td>
                        ))}
                      </tr>
                    ))}

                  {isError && (
                    <tr>
                      <td colSpan={8} className="px-4 py-10 text-center text-red-400 text-sm">
                        ⚠️ Failed to load attackers. Check API connection.
                      </td>
                    </tr>
                  )}

                  {!isLoading && !isError && filtered.length === 0 && (
                    <tr>
                      <td colSpan={8} className="px-4 py-12 text-center">
                        <span className="text-4xl block mb-3">👥</span>
                        <p className="text-slate-400 text-sm font-semibold">
                          {activeKeyword ? "No attackers found" : "Select a keyword to view attackers"}
                        </p>
                        <p className="text-slate-600 text-xs mt-1">
                          {activeKeyword && "Try adjusting your filters"}
                        </p>
                      </td>
                    </tr>
                  )}

                  {!isLoading && filtered.map((author) => (
                    <AttackerRow key={author.id} author={author} />
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
