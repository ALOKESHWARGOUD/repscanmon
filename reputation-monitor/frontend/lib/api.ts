import axios, { AxiosInstance } from "axios";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export const DEFAULT_USER_ID = "dashboard-user";

const api: AxiosInstance = axios.create({
  baseURL: `${API_BASE}/api/v1`,
  headers: { "Content-Type": "application/json" },
  timeout: 30000,
});

// Auth interceptor: attach JWT from localStorage
api.interceptors.request.use((config) => {
  if (typeof window !== "undefined") {
    const token = localStorage.getItem("auth_token");
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
  }
  return config;
});

// Response interceptor: handle 401 by re-acquiring a token
api.interceptors.response.use(
  (response) => response,
  async (error) => {
    if (error.response?.status === 401 && typeof window !== "undefined") {
      localStorage.removeItem("auth_token");
      try {
        const res = await axios.post<{ access_token: string }>(
          `${API_BASE}/api/v1/auth/token`,
          { user_id: DEFAULT_USER_ID }
        );
        localStorage.setItem("auth_token", res.data.access_token);
        // Retry the original request with the new token
        const retryConfig = {
          ...error.config,
          headers: {
            ...(error.config.headers ?? {}),
            Authorization: `Bearer ${res.data.access_token}`,
          },
        };
        return axios(retryConfig);
      } catch {
        window.location.href = "/";
      }
    }
    return Promise.reject(error);
  }
);

// ─── Types ────────────────────────────────────────────────────────────────────

// Supported platforms for filtering
export const SUPPORTED_PLATFORMS = ["twitter", "instagram", "youtube"] as const;
export type Platform = typeof SUPPORTED_PLATFORMS[number];

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
}

export interface Keyword {
  id: string;
  keyword: string;
  created_at: string;
  is_active: boolean;
  owner_user_id: string | null;
}

export interface SentimentSummary {
  keyword: string;
  positive_count: number;
  negative_count: number;
  neutral_count: number;
  total_count: number;
  negative_ratio: number;
  score: number;
  risk_level: "low" | "moderate" | "high";
}

export interface Post {
  id: string;
  platform: string;
  post_id: string;
  author_name: string;
  followers_count: number;
  content: string;
  posted_at: string;
  url: string;
  likes_count: number;
  replies_count: number;
  shares_count: number;
  language: string;
  sentiment: string | null;
  confidence: number | null;
}

export interface ReputationScore {
  id: string;
  keyword_id: string;
  score: number;
  positive_count: number;
  negative_count: number;
  neutral_count: number;
  total_count: number;
  negative_ratio: number;
  risk_level: string;
  computed_at: string;
}

export interface TrackedAuthor {
  id: string;
  platform: string;
  author_id: string;
  author_name: string;
  followers_count: number;
  negative_post_count: number;
  risk_score: number;
  is_flagged: boolean;
  last_seen_at: string;
}

export interface AttackCluster {
  id: string;
  keyword_id: string;
  detected_at: string;
  cluster_size: number;
  confidence_score: number;
  member_ids: string[];
  description: string | null;
  status: string;
}

export interface Alert {
  id: string;
  keyword_id: string;
  alert_type: string;
  message: string;
  evidence_url: string | null;
  sent_via: string[];
  triggered_at: string;
  is_read: boolean;
}

export interface TimelineDataPoint {
  hour: string;
  positive: number;
  negative: number;
  neutral: number;
}

// ─── API Methods ──────────────────────────────────────────────────────────────

// Keywords
export const keywordsApi = {
  list: (page = 1, pageSize = 20) =>
    api.get<PaginatedResponse<Keyword>>("/keywords", {
      params: { page, page_size: pageSize },
    }),
  create: (keyword: string) => api.post<Keyword>("/keywords", { keyword }),
  delete: (id: string) => api.delete(`/keywords/${id}`),
};

// Sentiment
export const sentimentApi = {
  getSummary: (keyword: string) =>
    api.get<SentimentSummary>(`/sentiment/${keyword}`),
  getTimeline: (keyword: string) =>
    api.get<TimelineDataPoint[]>(`/timeline/${keyword}`),
};

// Posts
export const postsApi = {
  list: (
    keyword: string,
    params?: {
      page?: number;
      page_size?: number;
      sentiment?: string;
      platform?: string;
    }
  ) => api.get<PaginatedResponse<Post>>(`/posts/${keyword}`, { params }),
};

// Scores
export const scoresApi = {
  getCurrent: (keyword: string) =>
    api.get<ReputationScore>(`/score/${keyword}`),
  getHistory: (keyword: string, days = 30) =>
    api.get<ReputationScore[]>(`/score/${keyword}/history`, {
      params: { days },
    }),
};

// Attackers
export const attackersApi = {
  list: (
    keyword: string,
    params?: { page?: number; platform?: string; flagged_only?: boolean }
  ) =>
    api.get<PaginatedResponse<TrackedAuthor>>(`/attackers/${keyword}`, {
      params,
    }),
};

// Clusters
export const clustersApi = {
  list: (keyword: string, page = 1) =>
    api.get<PaginatedResponse<AttackCluster>>(`/clusters/${keyword}`, {
      params: { page },
    }),
};

// Alerts
export const alertsApi = {
  list: (page = 1, pageSize = 20) =>
    api.get<PaginatedResponse<Alert>>("/alerts", {
      params: { page, page_size: pageSize },
    }),
  markRead: (id: string) => api.patch(`/alerts/${id}/read`),
  markAllRead: () => api.patch("/alerts/read-all"),
  subscribe: (email?: string, telegramChatId?: string) =>
    api.post("/alerts/subscribe", {
      email,
      telegram_chat_id: telegramChatId,
    }),
};

// Auth
export const authApi = {
  getToken: (userId: string) =>
    api.post<{ access_token: string; token_type: string }>("/auth/token", {
      user_id: userId,
    }),
};

export default api;
