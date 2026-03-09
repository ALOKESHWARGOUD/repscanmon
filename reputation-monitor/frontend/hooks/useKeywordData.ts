import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  keywordsApi,
  sentimentApi,
  postsApi,
  scoresApi,
  attackersApi,
  clustersApi,
  alertsApi,
} from "@/lib/api";

// ── Keywords ──────────────────────────────────────────────────────────────────

export function useKeywords(page = 1, pageSize = 20) {
  return useQuery({
    queryKey: ["keywords", page, pageSize],
    queryFn: () => keywordsApi.list(page, pageSize).then((r) => r.data),
    staleTime: 30_000,
  });
}

export function useCreateKeyword() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (keyword: string) =>
      keywordsApi.create(keyword).then((r) => r.data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["keywords"] }),
  });
}

export function useDeleteKeyword() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => keywordsApi.delete(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["keywords"] }),
  });
}

// ── Sentiment ─────────────────────────────────────────────────────────────────

export function useSentimentSummary(keyword: string) {
  return useQuery({
    queryKey: ["sentiment", keyword],
    queryFn: () => sentimentApi.getSummary(keyword).then((r) => r.data),
    enabled: !!keyword,
    staleTime: 60_000,
  });
}

export function useTimeline(keyword: string) {
  return useQuery({
    queryKey: ["timeline", keyword],
    queryFn: () => sentimentApi.getTimeline(keyword).then((r) => r.data),
    enabled: !!keyword,
    staleTime: 60_000,
  });
}

// ── Posts ─────────────────────────────────────────────────────────────────────

export function usePosts(
  keyword: string,
  params?: { page?: number; sentiment?: string; platform?: string }
) {
  return useQuery({
    queryKey: ["posts", keyword, params],
    queryFn: () => postsApi.list(keyword, params).then((r) => r.data),
    enabled: !!keyword,
    staleTime: 30_000,
  });
}

// ── Scores ────────────────────────────────────────────────────────────────────

export function useCurrentScore(keyword: string) {
  return useQuery({
    queryKey: ["score", keyword],
    queryFn: () => scoresApi.getCurrent(keyword).then((r) => r.data),
    enabled: !!keyword,
    staleTime: 60_000,
  });
}

export function useScoreHistory(keyword: string, days = 30) {
  return useQuery({
    queryKey: ["scoreHistory", keyword, days],
    queryFn: () => scoresApi.getHistory(keyword, days).then((r) => r.data),
    enabled: !!keyword,
    staleTime: 300_000,
  });
}

// ── Attackers ─────────────────────────────────────────────────────────────────

export function useAttackers(
  keyword: string,
  params?: { page?: number; platform?: string; flagged_only?: boolean }
) {
  return useQuery({
    queryKey: ["attackers", keyword, params],
    queryFn: () => attackersApi.list(keyword, params).then((r) => r.data),
    enabled: !!keyword,
    staleTime: 60_000,
  });
}

// ── Clusters ──────────────────────────────────────────────────────────────────

export function useClusters(keyword: string, page = 1) {
  return useQuery({
    queryKey: ["clusters", keyword, page],
    queryFn: () => clustersApi.list(keyword, page).then((r) => r.data),
    enabled: !!keyword,
    staleTime: 60_000,
  });
}

// ── Alerts ────────────────────────────────────────────────────────────────────

export function useAlerts(page = 1, pageSize = 20) {
  return useQuery({
    queryKey: ["alerts", page, pageSize],
    queryFn: () => alertsApi.list(page, pageSize).then((r) => r.data),
    staleTime: 30_000,
  });
}

export function useMarkAlertRead() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => alertsApi.markRead(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["alerts"] }),
  });
}

export function useMarkAllAlertsRead() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => alertsApi.markAllRead(),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["alerts"] }),
  });
}

export function useSubscribeAlerts() {
  return useMutation({
    mutationFn: ({
      email,
      telegramChatId,
    }: {
      email?: string;
      telegramChatId?: string;
    }) => alertsApi.subscribe(email, telegramChatId),
  });
}
