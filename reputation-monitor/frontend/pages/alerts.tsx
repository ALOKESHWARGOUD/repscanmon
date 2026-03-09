import { useState } from "react";
import Sidebar from "@/components/Sidebar";
import AlertsPanel from "@/components/AlertsPanel";
import { useMarkAllAlertsRead, useSubscribeAlerts, useAlerts } from "@/hooks/useKeywordData";

// ── Notification type config ───────────────────────────────────────────────────

const NOTIF_TYPES = [
  {
    id: "negative_spike",
    label: "Negative Spike",
    description: "Sudden increase in negative sentiment",
    icon: "📈",
  },
  {
    id: "attack_detected",
    label: "Attack Detected",
    description: "Coordinated attack cluster identified",
    icon: "🚨",
  },
  {
    id: "viral_negative",
    label: "Viral Negative",
    description: "Negative post going viral",
    icon: "🔥",
  },
  {
    id: "high_risk_author_active",
    label: "High-Risk Author Active",
    description: "Known bad actor posted about your keyword",
    icon: "⚠️",
  },
];

// ── Alert Settings Form ────────────────────────────────────────────────────────

function AlertSettingsPanel() {
  const [email, setEmail] = useState("");
  const [telegramId, setTelegramId] = useState("");
  const [enabledTypes, setEnabledTypes] = useState<Set<string>>(
    new Set(["negative_spike", "attack_detected", "viral_negative", "high_risk_author_active"])
  );
  const [saveStatus, setSaveStatus] = useState<"idle" | "saving" | "saved" | "error">("idle");

  const subscribe = useSubscribeAlerts();

  function toggleType(id: string) {
    setEnabledTypes((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  async function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!email && !telegramId) return;
    setSaveStatus("saving");
    try {
      await subscribe.mutateAsync({
        email: email || undefined,
        telegramChatId: telegramId || undefined,
      });
      setSaveStatus("saved");
      setTimeout(() => setSaveStatus("idle"), 2500);
    } catch {
      setSaveStatus("error");
      setTimeout(() => setSaveStatus("idle"), 3000);
    }
  }

  return (
    <div className="bg-slate-900 border border-slate-700/50 rounded-xl overflow-hidden h-fit">
      <div className="flex items-center gap-2 px-5 py-3.5 border-b border-slate-700/50">
        <span>⚙️</span>
        <h2 className="text-sm font-bold text-white">Alert Settings</h2>
      </div>

      <form onSubmit={handleSave} className="p-5 space-y-5">
        {/* Email */}
        <div className="space-y-1.5">
          <label className="block text-xs font-semibold text-slate-400 uppercase tracking-wider">
            Email Notifications
          </label>
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder="your@email.com"
            className="w-full rounded-lg bg-slate-800 border border-slate-600/50 px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500/70 focus:ring-1 focus:ring-blue-500/30 transition-colors"
          />
        </div>

        {/* Telegram */}
        <div className="space-y-1.5">
          <label className="block text-xs font-semibold text-slate-400 uppercase tracking-wider">
            Telegram Chat ID
          </label>
          <input
            type="text"
            value={telegramId}
            onChange={(e) => setTelegramId(e.target.value)}
            placeholder="-1001234567890"
            className="w-full rounded-lg bg-slate-800 border border-slate-600/50 px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500/70 focus:ring-1 focus:ring-blue-500/30 transition-colors"
          />
          <p className="text-[11px] text-slate-600">
            Use @userinfobot on Telegram to get your chat ID
          </p>
        </div>

        {/* Divider */}
        <div className="border-t border-slate-700/50" />

        {/* Notification types */}
        <div className="space-y-2">
          <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">
            Notification Types
          </p>
          {NOTIF_TYPES.map((nt) => (
            <label
              key={nt.id}
              className="flex items-start gap-3 cursor-pointer group"
            >
              <div className="mt-0.5 flex-shrink-0">
                <input
                  type="checkbox"
                  checked={enabledTypes.has(nt.id)}
                  onChange={() => toggleType(nt.id)}
                  className="w-4 h-4 rounded border-slate-600 bg-slate-800 text-blue-600 focus:ring-blue-500/30 focus:ring-1"
                />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-1.5">
                  <span className="text-sm">{nt.icon}</span>
                  <span className="text-sm font-medium text-slate-300 group-hover:text-white transition-colors">
                    {nt.label}
                  </span>
                </div>
                <p className="text-[11px] text-slate-600 mt-0.5">{nt.description}</p>
              </div>
            </label>
          ))}
        </div>

        {/* Save button */}
        <button
          type="submit"
          disabled={subscribe.isPending || (!email && !telegramId)}
          className={`w-full py-2.5 rounded-lg text-sm font-semibold transition-all ${
            saveStatus === "saved"
              ? "bg-emerald-600 text-white"
              : saveStatus === "error"
              ? "bg-red-600 text-white"
              : "bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-50 disabled:cursor-not-allowed"
          }`}
        >
          {saveStatus === "saving"
            ? "Saving…"
            : saveStatus === "saved"
            ? "✓ Preferences Saved"
            : saveStatus === "error"
            ? "✗ Save Failed"
            : "Save Preferences"}
        </button>
      </form>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function Alerts() {
  const markAllRead = useMarkAllAlertsRead();
  const { data: alertsData } = useAlerts(1, 1);
  const total = alertsData?.total ?? 0;
  const totalUnread = alertsData?.items?.filter((a) => !a.is_read).length ?? 0;

  return (
    <div className="flex min-h-screen bg-slate-950">
      <Sidebar />

      <div className="flex-1 ml-64 flex flex-col min-h-screen">
        {/* Header */}
        <header className="sticky top-0 z-30 flex h-16 items-center justify-between gap-4 border-b border-slate-700/50 bg-slate-950/90 backdrop-blur px-8">
          <div className="flex items-center gap-3">
            <h1 className="text-xl font-bold text-white">Alerts & Notifications</h1>
            {total > 0 && (
              <span className="px-2 py-0.5 rounded bg-slate-700 text-slate-400 text-xs font-bold">
                {total} total
              </span>
            )}
          </div>
          <button
            onClick={() => markAllRead.mutate()}
            disabled={markAllRead.isPending || totalUnread === 0}
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-slate-800 border border-slate-600/50 hover:border-slate-500 text-slate-300 hover:text-white text-sm font-semibold transition-all disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {markAllRead.isPending ? (
              "Marking…"
            ) : (
              <>
                <span>✓</span>
                <span>Mark All Read</span>
              </>
            )}
          </button>
        </header>

        <main className="flex-1 p-8">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            {/* Left: Alert history timeline — 2/3 */}
            <div className="lg:col-span-2">
              <div className="bg-slate-900 border border-slate-700/50 rounded-xl p-5">
                <AlertsPanel limit={20} />
              </div>
            </div>

            {/* Right: Settings — 1/3 */}
            <div className="lg:col-span-1">
              <AlertSettingsPanel />
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
