"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { formatDistanceToNow } from "date-fns";
import { useLiveFeed, LivePost, LiveStats } from "@/hooks/useLiveFeed";

// ── Platform helpers ──────────────────────────────────────────────────────────

const PLATFORM_ICONS: Record<string, string> = {
  twitter: "𝕏",
  reddit: "🔴",
  youtube: "🎬",
  news: "📰",
};

const PLATFORM_LABELS: Record<string, string> = {
  twitter: "Twitter/X",
  reddit: "Reddit",
  youtube: "YouTube",
  news: "News",
};

function platformIcon(platform: string): string {
  return PLATFORM_ICONS[platform?.toLowerCase()] ?? "🌐";
}

function platformLabel(platform: string): string {
  return PLATFORM_LABELS[platform?.toLowerCase()] ?? platform;
}

// ── Sub-components ────────────────────────────────────────────────────────────

function ConnectionBadge({
  status,
}: {
  status: "connecting" | "connected" | "disconnected" | "error";
}) {
  const map = {
    connected: { color: "text-emerald-400", dot: "bg-emerald-400", label: "Connected" },
    connecting: { color: "text-yellow-400", dot: "bg-yellow-400", label: "Connecting…" },
    disconnected: { color: "text-slate-400", dot: "bg-slate-400", label: "Disconnected" },
    error: { color: "text-red-400", dot: "bg-red-400", label: "Error" },
  };
  const cfg = map[status];

  return (
    <span className={`flex items-center gap-1.5 text-xs font-medium ${cfg.color}`}>
      <span
        className={`h-2 w-2 rounded-full ${cfg.dot} ${
          status === "connecting" ? "animate-pulse" : ""
        }`}
      />
      {cfg.label}
    </span>
  );
}

function StatsBar({ stats }: { stats: LiveStats }) {
  const riskColors: Record<string, string> = {
    low: "text-emerald-400 bg-emerald-400/10 border-emerald-400/30",
    moderate: "text-yellow-400 bg-yellow-400/10 border-yellow-400/30",
    high: "text-red-400 bg-red-400/10 border-red-400/30",
  };
  const riskColor = riskColors[stats.risk_level] ?? riskColors.low;

  return (
    <div className="grid grid-cols-5 gap-px bg-slate-700/40 rounded-lg overflow-hidden text-center text-xs">
      <div className="bg-slate-800/80 px-2 py-2">
        <p className="text-slate-400 mb-0.5">Score</p>
        <p
          className={`font-bold text-sm tabular-nums ${
            stats.reputation_score >= 0 ? "text-emerald-400" : "text-red-400"
          }`}
        >
          {stats.reputation_score > 0 ? "+" : ""}
          {stats.reputation_score.toFixed(1)}
        </p>
      </div>
      <div className="bg-slate-800/80 px-2 py-2">
        <p className="text-slate-400 mb-0.5">Positive</p>
        <p className="font-bold text-sm text-emerald-400 tabular-nums">
          {stats.positive_count.toLocaleString()}
        </p>
      </div>
      <div className="bg-slate-800/80 px-2 py-2">
        <p className="text-slate-400 mb-0.5">Negative</p>
        <p className="font-bold text-sm text-red-400 tabular-nums">
          {stats.negative_count.toLocaleString()}
        </p>
      </div>
      <div className="bg-slate-800/80 px-2 py-2">
        <p className="text-slate-400 mb-0.5">Neutral</p>
        <p className="font-bold text-sm text-blue-400 tabular-nums">
          {stats.neutral_count.toLocaleString()}
        </p>
      </div>
      <div className="bg-slate-800/80 px-2 py-2">
        <p className="text-slate-400 mb-0.5">Risk</p>
        <span
          className={`inline-block px-1.5 py-0.5 rounded border text-[10px] font-bold uppercase tracking-wide ${riskColor}`}
        >
          {stats.risk_level}
        </span>
      </div>
    </div>
  );
}

function SentimentBadge({
  sentiment,
  confidence,
}: {
  sentiment: "positive" | "negative" | "neutral";
  confidence: number;
}) {
  const cfg = {
    positive: "bg-emerald-500/20 text-emerald-300 border-emerald-500/30",
    negative: "bg-red-500/20 text-red-300 border-red-500/30",
    neutral: "bg-blue-500/20 text-blue-300 border-blue-500/30",
  }[sentiment];

  const labels = { positive: "POS", negative: "NEG", neutral: "NEU" };

  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-[11px] font-semibold ${cfg}`}
    >
      {labels[sentiment]}
      <span className="opacity-70">{Math.round(confidence * 100)}%</span>
    </span>
  );
}

// ── PostCard ──────────────────────────────────────────────────────────────────

function PostCard({ post, isNew }: { post: LivePost; isNew: boolean }) {
  const cardRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = cardRef.current;
    if (!el || !isNew) return;
    el.classList.add("animate-slide-in");
    const t = setTimeout(() => el.classList.remove("animate-slide-in"), 600);
    return () => clearTimeout(t);
  }, [isNew]);

  const bgMap = {
    negative: "bg-red-950/20 border-red-900/30",
    positive: "bg-green-950/10 border-green-900/20",
    neutral: "bg-slate-800/50 border-slate-700/30",
  };
  const bg = bgMap[post.sentiment] ?? bgMap.neutral;

  const timeAgo = (() => {
    try {
      return formatDistanceToNow(new Date(post.posted_at), { addSuffix: true });
    } catch {
      return "recently";
    }
  })();

  return (
    <div
      ref={cardRef}
      className={`rounded-lg border p-3 transition-all duration-300 ${bg} ${
        post.is_flagged_author ? "ring-1 ring-red-500/50" : ""
      }`}
    >
      {/* Header row */}
      <div className="flex items-center justify-between gap-2 mb-2">
        <div className="flex items-center gap-2 min-w-0">
          <span
            className="text-base leading-none flex-shrink-0"
            title={platformLabel(post.platform)}
          >
            {platformIcon(post.platform)}
          </span>
          <span className="text-xs font-semibold text-slate-200 truncate">
            {post.author_name}
          </span>
          {post.is_flagged_author && (
            <span className="flex-shrink-0 px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wide rounded bg-red-500/20 text-red-400 border border-red-500/30">
              ⚑ Flagged
            </span>
          )}
        </div>
        <SentimentBadge
          sentiment={post.sentiment}
          confidence={post.confidence}
        />
      </div>

      {/* Content */}
      <p className="text-xs text-slate-300 leading-relaxed line-clamp-2 mb-2">
        {post.content}
      </p>

      {/* Footer row */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-3 text-[11px] text-slate-500">
          {post.followers_count > 0 && (
            <span title="Followers">
              👥 {post.followers_count >= 1000
                ? `${(post.followers_count / 1000).toFixed(1)}k`
                : post.followers_count}
            </span>
          )}
          {post.likes_count > 0 && (
            <span title="Likes">
              ♥ {post.likes_count >= 1000
                ? `${(post.likes_count / 1000).toFixed(1)}k`
                : post.likes_count}
            </span>
          )}
          <span title={post.posted_at}>{timeAgo}</span>
        </div>
        {post.url && (
          <a
            href={post.url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-[11px] text-blue-400 hover:text-blue-300 transition-colors flex-shrink-0"
          >
            View original →
          </a>
        )}
      </div>
    </div>
  );
}

// ── Empty state ───────────────────────────────────────────────────────────────

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center h-full gap-4 text-center py-16">
      <div className="text-5xl animate-pulse">📡</div>
      <div>
        <p className="text-slate-300 font-semibold text-sm">Monitoring…</p>
        <p className="text-slate-500 text-xs mt-1">
          Waiting for incoming posts
        </p>
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

interface LiveFeedPanelProps {
  keyword: string;
}

export default function LiveFeedPanel({ keyword }: LiveFeedPanelProps) {
  const { posts, stats, connectionStatus, clearFeed } = useLiveFeed(keyword);
  const [newPostIds, setNewPostIds] = useState<Set<string>>(new Set());
  const prevCountRef = useRef(0);

  // Track which posts are freshly arrived for slide-in animation
  useEffect(() => {
    if (posts.length > prevCountRef.current) {
      const freshCount = posts.length - prevCountRef.current;
      const freshPosts = posts.slice(0, freshCount);
      setNewPostIds(
        (prev) =>
          new Set([
            ...prev,
            ...freshPosts.map((p) => `${p.author_name}_${p.posted_at}`),
          ])
      );
      // Remove animation flag after 800ms
      const t = setTimeout(() => {
        setNewPostIds(new Set());
      }, 800);
      prevCountRef.current = posts.length;
      return () => clearTimeout(t);
    }
    prevCountRef.current = posts.length;
  }, [posts]);

  const handleClear = useCallback(() => {
    clearFeed();
    prevCountRef.current = 0;
    setNewPostIds(new Set());
  }, [clearFeed]);

  return (
    <aside className="flex flex-col h-screen w-[360px] flex-shrink-0 bg-gray-900 border-l border-slate-700/50">
      {/* ── Header ── */}
      <div className="flex-shrink-0 px-4 pt-4 pb-3 border-b border-slate-700/50 space-y-3">
        {/* Title row */}
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <span className="relative flex h-2.5 w-2.5">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-500 opacity-75" />
              <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-red-500" />
            </span>
            <span className="text-sm font-bold text-white tracking-wide">
              LIVE
            </span>
            <span className="px-2 py-0.5 rounded-full bg-blue-500/20 border border-blue-500/30 text-blue-300 text-xs font-semibold truncate max-w-[120px]">
              #{keyword}
            </span>
          </div>
          <ConnectionBadge status={connectionStatus} />
        </div>

        {/* Stats bar */}
        {stats ? (
          <StatsBar stats={stats} />
        ) : (
          <div className="grid grid-cols-5 gap-px rounded-lg overflow-hidden">
            {Array.from({ length: 5 }).map((_, i) => (
              <div
                key={i}
                className="bg-slate-800/80 h-12 animate-pulse rounded-sm"
              />
            ))}
          </div>
        )}

        {/* Clear button */}
        <div className="flex items-center justify-between">
          <span className="text-xs text-slate-500">
            {posts.length} post{posts.length !== 1 ? "s" : ""} in feed
          </span>
          {posts.length > 0 && (
            <button
              onClick={handleClear}
              className="text-xs text-slate-400 hover:text-white transition-colors px-2 py-1 rounded hover:bg-slate-700/50"
            >
              Clear feed ✕
            </button>
          )}
        </div>
      </div>

      {/* ── Feed ── */}
      <div className="flex-1 overflow-y-auto overscroll-contain px-3 py-3 space-y-2 scrollbar-thin scrollbar-track-slate-900 scrollbar-thumb-slate-700">
        {posts.length === 0 ? (
          <EmptyState />
        ) : (
          posts.map((post) => {
            const id = `${post.author_name}_${post.posted_at}`;
            return (
              <PostCard
                key={id}
                post={post}
                isNew={newPostIds.has(id)}
              />
            );
          })
        )}
      </div>

      {/* ── Slide-in keyframe injection ── */}
      <style>{`
        @keyframes slideIn {
          from { opacity: 0; transform: translateY(-12px); }
          to   { opacity: 1; transform: translateY(0); }
        }
        .animate-slide-in {
          animation: slideIn 0.35s ease-out forwards;
        }
        .scrollbar-thin::-webkit-scrollbar { width: 4px; }
        .scrollbar-track-slate-900::-webkit-scrollbar-track { background: rgb(17 24 39); }
        .scrollbar-thumb-slate-700::-webkit-scrollbar-thumb {
          background: rgb(51 65 85);
          border-radius: 2px;
        }
      `}</style>
    </aside>
  );
}
