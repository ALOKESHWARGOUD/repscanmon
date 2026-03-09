"use client";

import Link from "next/link";
import { useRouter } from "next/router";
import { useEffect, useState } from "react";

interface NavItem {
  icon: string;
  label: string;
  href: string;
}

const NAV_ITEMS: NavItem[] = [
  { icon: "📊", label: "Dashboard", href: "/dashboard" },
  { icon: "🔑", label: "Keywords", href: "/dashboard" },
  { icon: "👥", label: "Attackers", href: "/attackers" },
  { icon: "🕸️", label: "Clusters", href: "/clusters" },
  { icon: "🔔", label: "Alerts", href: "/alerts" },
];

export default function Sidebar() {
  const router = useRouter();
  const [apiStatus, setApiStatus] = useState<"checking" | "online" | "offline">("checking");

  useEffect(() => {
    const apiBase = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
    fetch(`${apiBase}/health`, { signal: AbortSignal.timeout(4000) })
      .then((r) => setApiStatus(r.ok ? "online" : "offline"))
      .catch(() => setApiStatus("offline"));
  }, []);

  function isActive(href: string): boolean {
    if (href === "/dashboard") {
      return router.pathname === "/dashboard" || router.pathname === "/";
    }
    return router.pathname.startsWith(href);
  }

  return (
    <aside className="fixed inset-y-0 left-0 z-40 flex w-64 flex-col bg-gray-900 border-r border-slate-700/50">
      {/* Brand */}
      <div className="flex h-16 items-center gap-3 px-5 border-b border-slate-700/50">
        <span className="text-2xl select-none">🔍</span>
        <div className="flex flex-col leading-tight">
          <span className="text-white font-bold text-base tracking-tight">RepScanMon</span>
          <span className="text-slate-500 text-[11px] font-medium uppercase tracking-widest">
            Reputation Monitor
          </span>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto px-3 py-4 space-y-0.5">
        <p className="px-2 pb-2 text-[10px] font-bold uppercase tracking-widest text-slate-600">
          Navigation
        </p>
        {NAV_ITEMS.map((item) => {
          const active = isActive(item.href);
          return (
            <Link
              key={item.label}
              href={item.href}
              className={`group relative flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-all duration-150 ${
                active
                  ? "bg-blue-600/15 text-blue-400 border border-blue-500/20"
                  : "text-slate-400 hover:bg-slate-800/60 hover:text-slate-200 border border-transparent"
              }`}
            >
              {/* Active left accent */}
              {active && (
                <span className="absolute left-0 top-1/2 -translate-y-1/2 h-5 w-0.5 rounded-r bg-blue-500" />
              )}
              <span className="text-base leading-none select-none">{item.icon}</span>
              <span>{item.label}</span>
              {active && (
                <span className="ml-auto h-1.5 w-1.5 rounded-full bg-blue-400" />
              )}
            </Link>
          );
        })}
      </nav>

      {/* Bottom status */}
      <div className="p-4 border-t border-slate-700/50">
        <div className="rounded-lg bg-slate-800/60 border border-slate-700/40 px-3 py-2.5">
          <div className="flex items-center justify-between mb-1">
            <span className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider">
              API Status
            </span>
            <span
              className={`inline-flex items-center gap-1.5 text-[11px] font-bold ${
                apiStatus === "online"
                  ? "text-emerald-400"
                  : apiStatus === "offline"
                  ? "text-red-400"
                  : "text-yellow-400"
              }`}
            >
              <span
                className={`h-1.5 w-1.5 rounded-full ${
                  apiStatus === "online"
                    ? "bg-emerald-400 animate-pulse"
                    : apiStatus === "offline"
                    ? "bg-red-400"
                    : "bg-yellow-400 animate-pulse"
                }`}
              />
              {apiStatus === "online" ? "Online" : apiStatus === "offline" ? "Offline" : "Checking…"}
            </span>
          </div>
          <p className="text-[10px] text-slate-600 truncate">
            {process.env.NEXT_PUBLIC_API_URL ?? "localhost:8000"}
          </p>
        </div>
      </div>
    </aside>
  );
}
