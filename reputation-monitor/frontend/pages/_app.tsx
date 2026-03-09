import type { AppProps } from "next/app";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState, useEffect } from "react";
import "../styles/globals.css";
import { authApi, DEFAULT_USER_ID } from "@/lib/api";

export default function App({ Component, pageProps }: AppProps) {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: { queries: { retry: 1, staleTime: 30000 } },
      })
  );

  const [authReady, setAuthReady] = useState(false);

  useEffect(() => {
    if (typeof window === "undefined") {
      setAuthReady(true);
      return;
    }
    const existing = localStorage.getItem("auth_token");
    if (existing) {
      setAuthReady(true);
      return;
    }
    authApi
      .getToken(DEFAULT_USER_ID)
      .then((res) => {
        localStorage.setItem("auth_token", res.data.access_token);
      })
      .catch(() => {
        // Backend unavailable; proceed so error states are visible
      })
      .finally(() => {
        setAuthReady(true);
      });
  }, []);

  if (!authReady) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-slate-950">
        <div className="text-slate-400 text-sm animate-pulse">Connecting…</div>
      </div>
    );
  }

  return (
    <QueryClientProvider client={queryClient}>
      <Component {...pageProps} />
    </QueryClientProvider>
  );
}
