import { useState, useEffect, useRef, useCallback } from "react";

export interface LivePost {
  platform: string;
  author_name: string;
  content: string;
  url: string;
  posted_at: string;
  sentiment: "positive" | "negative" | "neutral";
  confidence: number;
  followers_count: number;
  likes_count: number;
  is_flagged_author: boolean;
}

export interface LiveStats {
  positive_count: number;
  negative_count: number;
  neutral_count: number;
  reputation_score: number;
  negative_ratio: number;
  risk_level: "low" | "moderate" | "high";
  total_last_hour: number;
}

const WS_BASE = process.env.NEXT_PUBLIC_WS_URL ?? "ws://localhost:8000";
const MAX_POSTS = 200;
const RECONNECT_DELAY_MS = 5000;

export function useLiveFeed(keyword: string) {
  const [posts, setPosts] = useState<LivePost[]>([]);
  const [stats, setStats] = useState<LiveStats | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<
    "connecting" | "connected" | "disconnected" | "error"
  >("disconnected");
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<NodeJS.Timeout | null>(null);

  const connect = useCallback(() => {
    if (!keyword) return;
    setConnectionStatus("connecting");
    const slug = keyword.toLowerCase().replace(/\s+/g, "_");
    const token =
      typeof window !== "undefined" ? localStorage.getItem("auth_token") : "";
    const ws = new WebSocket(`${WS_BASE}/ws/live/${slug}?token=${token}`);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnectionStatus("connected");
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
    };

    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data as string) as {
        event: string;
        data: LivePost | LiveStats;
      };
      if (msg.event === "new_post") {
        setPosts((prev) =>
          [msg.data as LivePost, ...prev].slice(0, MAX_POSTS)
        );
      }
      if (msg.event === "stats_update") {
        setStats(msg.data as LiveStats);
      }
    };

    ws.onerror = () => setConnectionStatus("error");

    ws.onclose = () => {
      setConnectionStatus("disconnected");
      reconnectTimer.current = setTimeout(connect, RECONNECT_DELAY_MS);
    };
  }, [keyword]);

  useEffect(() => {
    connect();
    return () => {
      wsRef.current?.close();
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
    };
  }, [connect]);

  return {
    posts,
    stats,
    connectionStatus,
    isConnected: connectionStatus === "connected",
    clearFeed: () => setPosts([]),
  };
}
