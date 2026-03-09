"use client";

import { useMemo, useState } from "react";
import { format, parseISO } from "date-fns";
import { usePosts } from "@/hooks/useKeywordData";
import { Post } from "@/lib/api";

// ── Platform helpers ──────────────────────────────────────────────────────────

const PLATFORM_ICONS: Record<string, string> = {
  twitter: "🐦",
  reddit: "🔴",
  youtube: "🎬",
  news: "📰",
};

function platformIcon(p: string): string {
  return PLATFORM_ICONS[p?.toLowerCase()] ?? "🌐";
}

const ALL_PLATFORMS = ["all", "twitter", "reddit", "youtube", "news"];

// ── Sort helpers ──────────────────────────────────────────────────────────────

type SortKey = "confidence" | "likes_count" | "followers_count" | "posted_at";
type SortDir = "asc" | "desc";

function sortPosts(posts: Post[], key: SortKey, dir: SortDir): Post[] {
  return [...posts].sort((a, b) => {
    let av: number | string = 0;
    let bv: number | string = 0;
    if (key === "confidence") {
      av = a.confidence ?? 0;
      bv = b.confidence ?? 0;
    } else if (key === "likes_count") {
      av = a.likes_count ?? 0;
      bv = b.likes_count ?? 0;
    } else if (key === "followers_count") {
      av = a.followers_count ?? 0;
      bv = b.followers_count ?? 0;
    } else if (key === "posted_at") {
      av = a.posted_at ?? "";
      bv = b.posted_at ?? "";
    }
    if (av < bv) return dir === "asc" ? -1 : 1;
    if (av > bv) return dir === "asc" ? 1 : -1;
    return 0;
  });
}

// ── Column header ─────────────────────────────────────────────────────────────

interface ThProps {
  label: string;
  sortKey?: SortKey;
  current: SortKey;
  direction: SortDir;
  onSort: (k: SortKey) => void;
  className?: string;
}

function Th({ label, sortKey, current, direction, onSort, className = "" }: ThProps) {
  const isActive = sortKey && current === sortKey;
  return (
    <th
      className={`px-3 py-2.5 text-left text-xs font-semibold uppercase tracking-wide text-slate-400 whitespace-nowrap select-none ${
        sortKey
          ? "cursor-pointer hover:text-white transition-colors"
          : ""
      } ${className}`}
      onClick={() => sortKey && onSort(sortKey)}
    >
      <span className="flex items-center gap-1">
        {label}
        {sortKey && (
          <span className={isActive ? "text-blue-400" : "text-slate-600"}>
            {isActive ? (direction === "desc" ? "↓" : "↑") : "⇅"}
          </span>
        )}
      </span>
    </th>
  );
}

// ── Confidence badge ──────────────────────────────────────────────────────────

function ConfidenceBadge({ value }: { value: number | null }) {
  if (value === null) return <span className="text-slate-500">—</span>;
  const pct = Math.round(value * 100);
  const high = pct >= 80;
  return (
    <span
      className={`inline-block px-2 py-0.5 rounded text-xs font-bold tabular-nums ${
        high
          ? "bg-red-500/20 text-red-400 border border-red-500/30"
          : "bg-orange-500/15 text-orange-400 border border-orange-500/20"
      }`}
    >
      {pct}%
    </span>
  );
}

// ── Skeleton row ──────────────────────────────────────────────────────────────

function SkeletonRow() {
  return (
    <tr className="border-t border-slate-700/40 animate-pulse">
      {Array.from({ length: 7 }).map((_, i) => (
        <td key={i} className="px-3 py-3">
          <div className="h-3 bg-slate-700/50 rounded w-full" />
        </td>
      ))}
    </tr>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

interface TopNegativePostsListProps {
  keyword: string;
}

export default function TopNegativePostsList({
  keyword,
}: TopNegativePostsListProps) {
  const [sortKey, setSortKey] = useState<SortKey>("confidence");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [platformFilter, setPlatformFilter] = useState<string>("all");
  const [page, setPage] = useState(1);

  const { data, isLoading, isError } = usePosts(keyword, {
    sentiment: "negative",
    platform: platformFilter === "all" ? undefined : platformFilter,
    page,
  });

  const posts = data?.items ?? [];
  const total = data?.total ?? 0;
  const pageSize = data?.page_size ?? 20;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  const sortedPosts = useMemo(
    () => sortPosts(posts, sortKey, sortDir),
    [posts, sortKey, sortDir]
  );

  function handleSort(key: SortKey) {
    if (key === sortKey) {
      setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  }

  return (
    <div className="flex flex-col gap-3">
      {/* Controls */}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2">
          <label className="text-xs text-slate-400 font-medium">Platform:</label>
          <select
            value={platformFilter}
            onChange={(e) => {
              setPlatformFilter(e.target.value);
              setPage(1);
            }}
            className="bg-slate-800 border border-slate-600/50 text-slate-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:ring-1 focus:ring-blue-500 capitalize"
          >
            {ALL_PLATFORMS.map((p) => (
              <option key={p} value={p}>
                {p === "all" ? "All Platforms" : `${platformIcon(p)} ${p}`}
              </option>
            ))}
          </select>
        </div>
        <span className="text-xs text-slate-500">
          {total.toLocaleString()} negative post{total !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Table */}
      <div className="overflow-x-auto rounded-xl border border-slate-700/50">
        <table className="w-full text-sm">
          <thead className="bg-slate-800/80">
            <tr>
              <Th
                label="Platform"
                current={sortKey}
                direction={sortDir}
                onSort={handleSort}
              />
              <Th
                label="Author"
                current={sortKey}
                direction={sortDir}
                onSort={handleSort}
              />
              <th className="px-3 py-2.5 text-left text-xs font-semibold uppercase tracking-wide text-slate-400">
                Content
              </th>
              <Th
                label="Confidence"
                sortKey="confidence"
                current={sortKey}
                direction={sortDir}
                onSort={handleSort}
              />
              <Th
                label="Likes"
                sortKey="likes_count"
                current={sortKey}
                direction={sortDir}
                onSort={handleSort}
              />
              <Th
                label="Date"
                sortKey="posted_at"
                current={sortKey}
                direction={sortDir}
                onSort={handleSort}
              />
              <th className="px-3 py-2.5 text-left text-xs font-semibold uppercase tracking-wide text-slate-400">
                Link
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/30">
            {isLoading &&
              Array.from({ length: 8 }).map((_, i) => <SkeletonRow key={i} />)}

            {isError && (
              <tr>
                <td
                  colSpan={7}
                  className="px-4 py-8 text-center text-red-400 text-sm"
                >
                  ⚠ Failed to load posts. Please try again.
                </td>
              </tr>
            )}

            {!isLoading && !isError && sortedPosts.length === 0 && (
              <tr>
                <td
                  colSpan={7}
                  className="px-4 py-12 text-center text-slate-500 text-sm"
                >
                  <div className="flex flex-col items-center gap-2">
                    <span className="text-2xl">✅</span>
                    <span>No negative posts found</span>
                  </div>
                </td>
              </tr>
            )}

            {!isLoading &&
              sortedPosts.map((post) => {
                const dateStr = (() => {
                  try {
                    return format(parseISO(post.posted_at), "MMM d, HH:mm");
                  } catch {
                    return post.posted_at;
                  }
                })();

                const truncated =
                  (post.content ?? "").length > 100
                    ? post.content.slice(0, 100) + "…"
                    : post.content;

                return (
                  <tr
                    key={post.id}
                    className="bg-slate-900/40 hover:bg-slate-800/60 transition-colors"
                  >
                    {/* Platform */}
                    <td className="px-3 py-3 whitespace-nowrap">
                      <span className="flex items-center gap-1.5 text-slate-300 text-xs">
                        <span>{platformIcon(post.platform)}</span>
                        <span className="capitalize text-slate-400">
                          {post.platform}
                        </span>
                      </span>
                    </td>

                    {/* Author */}
                    <td className="px-3 py-3 whitespace-nowrap">
                      <span className="text-slate-200 text-xs font-medium">
                        {post.author_name}
                      </span>
                      {post.followers_count > 0 && (
                        <p className="text-slate-500 text-[11px]">
                          {post.followers_count >= 1000
                            ? `${(post.followers_count / 1000).toFixed(1)}k`
                            : post.followers_count}{" "}
                          followers
                        </p>
                      )}
                    </td>

                    {/* Content */}
                    <td className="px-3 py-3 max-w-[280px]">
                      <p
                        className="text-slate-300 text-xs leading-relaxed"
                        title={post.content}
                      >
                        {truncated}
                      </p>
                    </td>

                    {/* Confidence */}
                    <td className="px-3 py-3 whitespace-nowrap">
                      <ConfidenceBadge value={post.confidence} />
                    </td>

                    {/* Likes */}
                    <td className="px-3 py-3 whitespace-nowrap text-xs text-slate-400 tabular-nums">
                      {post.likes_count > 0
                        ? post.likes_count >= 1000
                          ? `${(post.likes_count / 1000).toFixed(1)}k`
                          : post.likes_count
                        : "—"}
                    </td>

                    {/* Date */}
                    <td className="px-3 py-3 whitespace-nowrap text-xs text-slate-400">
                      {dateStr}
                    </td>

                    {/* Link */}
                    <td className="px-3 py-3 whitespace-nowrap">
                      {post.url ? (
                        <a
                          href={post.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="inline-flex items-center gap-1 text-blue-400 hover:text-blue-300 text-xs transition-colors"
                        >
                          ↗ View
                        </a>
                      ) : (
                        <span className="text-slate-600 text-xs">—</span>
                      )}
                    </td>
                  </tr>
                );
              })}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between text-xs text-slate-400">
          <span>
            Page {page} of {totalPages}
          </span>
          <div className="flex items-center gap-1">
            <button
              disabled={page <= 1}
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              className="px-3 py-1.5 rounded bg-slate-800 border border-slate-600/50 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              ← Prev
            </button>
            <button
              disabled={page >= totalPages}
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              className="px-3 py-1.5 rounded bg-slate-800 border border-slate-600/50 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              Next →
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
