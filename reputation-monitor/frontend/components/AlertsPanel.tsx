"use client";

import { useState, useMemo } from "react";
import { formatDistanceToNow, parseISO } from "date-fns";
import {
  useAlerts,
  useMarkAlertRead,
  useMarkAllAlertsRead,
} from "@/hooks/useKeywordData";
import { Alert } from "@/lib/api";

// ── Alert type config ─────────────────────────────────────────────────────────

type AlertType =
  | "attack_detected"
  | "negative_spike"
  | "viral_negative"
  | "high_risk_author_active"
  | string;

interface AlertConfig {
  icon: string;
  label: string;
  borderColor: string;
  badgeClass: string;
  dotClass: string;
}

const ALERT_CONFIG: Record<string, AlertConfig> = {
  attack_detected: {
    icon: "🚨",
    label: "Attack Detected",
    borderColor: "border-red-500",
    badgeClass: "bg-red-500/20 text-red-400 border-red-500/30",
    dotClass: "bg-red-500",
  },
  negative_spike: {
    icon: "📈",
    label: "Negative Spike",
    borderColor: "border-orange-500",
    badgeClass: "bg-orange-500/20 text-orange-400 border-orange-500/30",
    dotClass: "bg-orange-500",
  },
  viral_negative: {
    icon: "🔥",
    label: "Viral Negative",
    borderColor: "border-purple-500",
    badgeClass: "bg-purple-500/20 text-purple-400 border-purple-500/30",
    dotClass: "bg-purple-500",
  },
  high_risk_author_active: {
    icon: "⚠️",
    label: "High Risk Author",
    borderColor: "border-yellow-500",
    badgeClass: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30",
    dotClass: "bg-yellow-400",
  },
};

const DEFAULT_CONFIG: AlertConfig = {
  icon: "🔔",
  label: "Alert",
  borderColor: "border-slate-500",
  badgeClass: "bg-slate-500/20 text-slate-400 border-slate-500/30",
  dotClass: "bg-slate-400",
};

function alertConfig(type: AlertType): AlertConfig {
  return ALERT_CONFIG[type] ?? DEFAULT_CONFIG;
}

// ── Alert card ────────────────────────────────────────────────────────────────

interface AlertCardProps {
  alert: Alert;
  onRead: (id: string) => void;
}

function AlertCard({ alert, onRead }: AlertCardProps) {
  const cfg = alertConfig(alert.alert_type);

  const timeAgo = (() => {
    try {
      return formatDistanceToNow(parseISO(alert.triggered_at), {
        addSuffix: true,
      });
    } catch {
      return "recently";
    }
  })();

  return (
    <div
      className={`relative flex gap-3 pl-4 border-l-2 ${cfg.borderColor} transition-opacity duration-300 ${
        alert.is_read ? "opacity-40" : "opacity-100"
      }`}
    >
      {/* Timeline dot */}
      <span
        className={`absolute -left-[5px] top-2 h-2.5 w-2.5 rounded-full border-2 border-slate-900 ${cfg.dotClass} flex-shrink-0`}
      />

      {/* Card body */}
      <div
        className={`flex-1 bg-slate-800/50 border border-slate-700/40 rounded-lg p-3 ${
          !alert.is_read
            ? "hover:border-slate-600/60 transition-colors"
            : ""
        }`}
      >
        {/* Header */}
        <div className="flex items-start justify-between gap-2 mb-2">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-base leading-none">{cfg.icon}</span>
            <span
              className={`inline-flex items-center px-2 py-0.5 rounded border text-[11px] font-bold uppercase tracking-wide ${cfg.badgeClass}`}
            >
              {cfg.label}
            </span>
            {!alert.is_read && (
              <span className="flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-2 w-2 rounded-full bg-blue-400 opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500" />
              </span>
            )}
          </div>
          <span className="text-[11px] text-slate-500 whitespace-nowrap flex-shrink-0">
            {timeAgo}
          </span>
        </div>

        {/* Message */}
        <p className="text-sm text-slate-300 leading-relaxed mb-2">
          {alert.message}
        </p>

        {/* Footer */}
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-3">
            {alert.evidence_url && (
              <a
                href={alert.evidence_url}
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-blue-400 hover:text-blue-300 transition-colors"
              >
                View evidence →
              </a>
            )}
            {alert.sent_via?.length > 0 && (
              <span className="text-[11px] text-slate-600">
                via {alert.sent_via.join(", ")}
              </span>
            )}
          </div>

          {!alert.is_read && (
            <button
              onClick={() => onRead(alert.id)}
              className="text-[11px] text-slate-500 hover:text-slate-300 transition-colors px-2 py-0.5 rounded hover:bg-slate-700/50"
            >
              Mark read
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

// ── Skeleton ──────────────────────────────────────────────────────────────────

function AlertSkeleton() {
  return (
    <div className="relative flex gap-3 pl-4 border-l-2 border-slate-700 animate-pulse">
      <div className="absolute -left-[5px] top-2 h-2.5 w-2.5 rounded-full bg-slate-700" />
      <div className="flex-1 bg-slate-800/50 border border-slate-700/40 rounded-lg p-3 space-y-2">
        <div className="flex items-center justify-between">
          <div className="h-4 w-28 bg-slate-700/50 rounded" />
          <div className="h-3 w-16 bg-slate-700/50 rounded" />
        </div>
        <div className="h-3 w-full bg-slate-700/50 rounded" />
        <div className="h-3 w-3/4 bg-slate-700/50 rounded" />
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

interface AlertsPanelProps {
  limit?: number;
}

export default function AlertsPanel({ limit = 10 }: AlertsPanelProps) {
  const [page, setPage] = useState(1);

  const { data, isLoading, isError } = useAlerts(page, limit);
  const markRead = useMarkAlertRead();
  const markAllRead = useMarkAllAlertsRead();

  const alerts: Alert[] = data?.items ?? [];
  const total = data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / limit));

  const unreadCount = useMemo(
    () => alerts.filter((a) => !a.is_read).length,
    [alerts]
  );

  function handleRead(id: string) {
    markRead.mutate(id);
  }

  function handleMarkAll() {
    markAllRead.mutate();
  }

  return (
    <div className="flex flex-col gap-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-bold text-white">Alerts</h3>
          {unreadCount > 0 && (
            <span className="px-2 py-0.5 bg-red-500/20 border border-red-500/30 text-red-400 text-xs font-bold rounded-full">
              {unreadCount} new
            </span>
          )}
        </div>
        {unreadCount > 0 && (
          <button
            onClick={handleMarkAll}
            disabled={markAllRead.isPending}
            className="text-xs text-slate-400 hover:text-white transition-colors px-3 py-1.5 rounded-lg bg-slate-800 border border-slate-600/50 hover:border-slate-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {markAllRead.isPending ? "Marking…" : "Mark all read ✓"}
          </button>
        )}
      </div>

      {/* Timeline */}
      <div className="flex flex-col gap-4">
        {isLoading &&
          Array.from({ length: 4 }).map((_, i) => (
            <AlertSkeleton key={i} />
          ))}

        {isError && (
          <div className="flex flex-col items-center justify-center gap-3 py-8 text-center">
            <span className="text-2xl">⚠️</span>
            <p className="text-red-400 text-sm">Failed to load alerts</p>
          </div>
        )}

        {!isLoading && !isError && alerts.length === 0 && (
          <div className="flex flex-col items-center justify-center gap-3 py-10 text-center">
            <span className="text-4xl">🔕</span>
            <div>
              <p className="text-slate-300 font-semibold text-sm">All clear</p>
              <p className="text-slate-500 text-xs mt-1">
                No alerts at this time
              </p>
            </div>
          </div>
        )}

        {!isLoading &&
          alerts.map((alert) => (
            <AlertCard key={alert.id} alert={alert} onRead={handleRead} />
          ))}
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between text-xs text-slate-400 pt-1">
          <span>
            Page {page} of {totalPages} · {total} total
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
