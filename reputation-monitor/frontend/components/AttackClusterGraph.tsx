"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ForceGraph2D } from "react-force-graph";
import { useAttackers, useClusters } from "@/hooks/useKeywordData";
import { TrackedAuthor, AttackCluster } from "@/lib/api";

// Cast ForceGraph2D to a regular React component since FCwithRef doesn't
// satisfy standard JSX element type constraints.
const FG2D = ForceGraph2D as unknown as React.ComponentType<Record<string, unknown>>;

// ── Types ─────────────────────────────────────────────────────────────────────

interface GraphNode {
  id: string;
  author: TrackedAuthor;
  clusterId?: string;
  color: string;
  size: number;
  x?: number;
  y?: number;
  [key: string]: unknown;
}

interface GraphLink {
  source: string;
  target: string;
  clusterId: string;
  [key: string]: unknown;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function riskColor(score: number, isFlagged: boolean): string {
  if (isFlagged) return "#EF4444";
  if (score > 60) return "#EF4444";
  if (score > 30) return "#F59E0B";
  return "#10B981";
}

function nodeSize(followers: number): number {
  const MIN = 4;
  const MAX = 16;
  // logarithmic scale: 0 followers → 4, 1M+ → 16
  if (followers <= 0) return MIN;
  const ratio = Math.log10(followers + 1) / Math.log10(1_000_001);
  return MIN + Math.round(ratio * (MAX - MIN));
}

// ── Author popup ──────────────────────────────────────────────────────────────

interface AuthorPopupProps {
  author: TrackedAuthor;
  onClose: () => void;
}

function AuthorPopup({ author, onClose }: AuthorPopupProps) {
  const riskColors: Record<string, string> = {
    low: "text-emerald-400",
    mid: "text-yellow-400",
    high: "text-red-400",
  };
  const risk =
    author.risk_score > 60 ? "high" : author.risk_score > 30 ? "mid" : "low";

  return (
    <div className="absolute bottom-4 left-4 z-10 bg-slate-800 border border-slate-600/50 rounded-xl p-4 shadow-2xl w-64">
      {/* Header */}
      <div className="flex items-start justify-between mb-3">
        <div>
          <p className="text-white font-semibold text-sm">{author.author_name}</p>
          <p className="text-slate-400 text-xs capitalize mt-0.5">
            📡 {author.platform}
          </p>
        </div>
        <button
          onClick={onClose}
          className="text-slate-500 hover:text-white transition-colors p-0.5"
          aria-label="Close"
        >
          ✕
        </button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 gap-2 mb-3">
        <div className="bg-slate-700/50 rounded-lg p-2 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wide">
            Followers
          </p>
          <p className="text-xs font-bold text-slate-200 mt-0.5 tabular-nums">
            {author.followers_count >= 1000
              ? `${(author.followers_count / 1000).toFixed(1)}k`
              : author.followers_count}
          </p>
        </div>
        <div className="bg-slate-700/50 rounded-lg p-2 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wide">
            Neg. Posts
          </p>
          <p className="text-xs font-bold text-red-400 mt-0.5 tabular-nums">
            {author.negative_post_count}
          </p>
        </div>
        <div className="bg-slate-700/50 rounded-lg p-2 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wide">
            Risk Score
          </p>
          <p
            className={`text-xs font-bold mt-0.5 tabular-nums ${
              riskColors[risk]
            }`}
          >
            {author.risk_score.toFixed(1)}
          </p>
        </div>
        <div className="bg-slate-700/50 rounded-lg p-2 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wide">
            Status
          </p>
          <p
            className={`text-xs font-bold mt-0.5 ${
              author.is_flagged ? "text-red-400" : "text-slate-300"
            }`}
          >
            {author.is_flagged ? "⚑ Flagged" : "Normal"}
          </p>
        </div>
      </div>
    </div>
  );
}

// ── Empty state ───────────────────────────────────────────────────────────────

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center h-full gap-4 text-center">
      <span className="text-5xl">🛡️</span>
      <div>
        <p className="text-slate-300 font-semibold text-sm">
          No attack clusters detected
        </p>
        <p className="text-slate-500 text-xs mt-1">
          The reputation graph is clean
        </p>
      </div>
    </div>
  );
}

// ── Legend ────────────────────────────────────────────────────────────────────

function GraphLegend() {
  const items = [
    { color: "#10B981", label: "Low risk (< 30)" },
    { color: "#F59E0B", label: "Medium risk (30–60)" },
    { color: "#EF4444", label: "High risk / Flagged (> 60)" },
  ];
  return (
    <div className="absolute top-3 right-3 bg-slate-800/90 border border-slate-600/40 rounded-lg px-3 py-2 text-xs space-y-1.5">
      {items.map((item) => (
        <div key={item.label} className="flex items-center gap-2">
          <span
            className="h-2.5 w-2.5 rounded-full flex-shrink-0"
            style={{ background: item.color }}
          />
          <span className="text-slate-400">{item.label}</span>
        </div>
      ))}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

interface AttackClusterGraphProps {
  keyword: string;
}

export default function AttackClusterGraph({
  keyword,
}: AttackClusterGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const fgRef = useRef<unknown>(undefined);
  const [dimensions, setDimensions] = useState({ width: 600, height: 400 });
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);

  // Observe container size
  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      if (width > 0 && height > 0) setDimensions({ width, height });
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  const { data: attackersData, isLoading: loadingAttackers } = useAttackers(
    keyword,
    { page: 1 }
  );
  const { data: clustersData, isLoading: loadingClusters } =
    useClusters(keyword, 1);

  const authors: TrackedAuthor[] = attackersData?.items ?? [];
  const clusters: AttackCluster[] = clustersData?.items ?? [];

  // Build graph data
  const graphData = useMemo(() => {
    if (!authors.length || !clusters.length) return { nodes: [], links: [] };

    const authorMap = new Map<string, TrackedAuthor>(
      authors.map((a) => [a.id, a])
    );

    const nodes: GraphNode[] = authors.map((a) => ({
      id: a.id,
      author: a,
      color: riskColor(a.risk_score, a.is_flagged),
      size: nodeSize(a.followers_count),
    }));

    const links: GraphLink[] = [];
    const seen = new Set<string>();

    clusters.forEach((cluster) => {
      const members = cluster.member_ids.filter((id) => authorMap.has(id));
      // Connect all members in same cluster as a ring
      for (let i = 0; i < members.length; i++) {
        for (let j = i + 1; j < members.length; j++) {
          const key = [members[i], members[j]].sort().join("|");
          if (!seen.has(key)) {
            seen.add(key);
            links.push({
              source: members[i],
              target: members[j],
              clusterId: cluster.id,
            });
          }
        }
      }
    });

    return { nodes, links };
  }, [authors, clusters]);

  const isLoading = loadingAttackers || loadingClusters;
  const isEmpty =
    !isLoading && (graphData.nodes.length === 0 || graphData.links.length === 0);

  const handleNodeClick = useCallback((node: Record<string, unknown>) => {
    const gn = node as unknown as GraphNode;
    setSelectedNode((prev) =>
      prev?.id === gn.id ? null : gn
    );
  }, []);

  const handleNodeHover = useCallback(
    (node: Record<string, unknown> | null) => {
      if (containerRef.current) {
        containerRef.current.style.cursor = node ? "pointer" : "default";
      }
    },
    []
  );

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full gap-3">
        <div className="h-5 w-5 rounded-full border-2 border-blue-500 border-t-transparent animate-spin" />
        <span className="text-slate-400 text-sm">Building attack graph…</span>
      </div>
    );
  }

  if (isEmpty) {
    return <EmptyState />;
  }

  return (
    <div
      ref={containerRef}
      className="relative w-full h-full bg-gray-900 rounded-xl overflow-hidden"
    >
      <FG2D
        ref={fgRef}
        width={dimensions.width}
        height={dimensions.height}
        graphData={graphData}
        backgroundColor="transparent"
        nodeColor={(node: Record<string, unknown>) => (node as unknown as GraphNode).color}
        nodeVal={(node: Record<string, unknown>) => (node as unknown as GraphNode).size}
        nodeLabel={(node: Record<string, unknown>) => (node as unknown as GraphNode).author.author_name}
        nodeCanvasObjectMode={() => "after"}
        nodeCanvasObject={(node: Record<string, unknown>, ctx: CanvasRenderingContext2D, globalScale: number) => {
          const gn = node as unknown as GraphNode;
          if (globalScale >= 1.5) {
            const label = gn.author.author_name;
            const fontSize = 10 / globalScale;
            ctx.font = `${fontSize}px Inter, sans-serif`;
            ctx.fillStyle = "rgba(255,255,255,0.8)";
            ctx.textAlign = "center";
            ctx.textBaseline = "middle";
            ctx.fillText(label, gn.x ?? 0, (gn.y ?? 0) + gn.size + 4);
          }
        }}
        linkColor={() => "rgba(148,163,184,0.15)"}
        linkWidth={1}
        onNodeClick={handleNodeClick}
        onNodeHover={handleNodeHover}
        enableNodeDrag
        enableZoomInteraction
        cooldownTicks={80}
        d3AlphaDecay={0.02}
        d3VelocityDecay={0.4}
      />

      <GraphLegend />

      {selectedNode && (
        <AuthorPopup
          author={selectedNode.author}
          onClose={() => setSelectedNode(null)}
        />
      )}

      {/* Stats overlay */}
      <div className="absolute top-3 left-3 bg-slate-800/90 border border-slate-600/40 rounded-lg px-3 py-2 text-xs space-y-1">
        <p className="text-slate-400">
          <span className="text-white font-bold">{graphData.nodes.length}</span>{" "}
          tracked authors
        </p>
        <p className="text-slate-400">
          <span className="text-red-400 font-bold">{clusters.length}</span>{" "}
          cluster{clusters.length !== 1 ? "s" : ""} detected
        </p>
      </div>
    </div>
  );
}
