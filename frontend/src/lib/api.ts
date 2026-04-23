// Thin API client for the RAG_Journey backend (FastAPI).
// All routes are relative so the React app can be served from the same origin.

const API = "";

function authHeaders(token: string | null): HeadersInit {
  return token ? { Authorization: `Bearer ${token}` } : {};
}

export async function apiFetch(path: string, init: RequestInit = {}): Promise<Response> {
  return fetch(`${API}${path}`, init);
}

export async function adminFetch(
  path: string,
  token: string | null,
  init: RequestInit = {},
): Promise<Response> {
  const headers = new Headers(init.headers || {});
  if (token) headers.set("Authorization", `Bearer ${token}`);
  return fetch(`${API}${path}`, { ...init, headers });
}

export interface Stats {
  total_files: number;
  total_chunks: number;
  departments: string[];
}

export async function fetchStats(token: string | null): Promise<Stats | null> {
  try {
    const url = token ? "/api/admin/stats" : "/api/stats";
    const res = await (token ? adminFetch(url, token) : apiFetch(url));
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export interface ChatResponse {
  answer: string;
  sources?: string[];
  rewritten_query?: string;
}

export async function startSession(userName: string, lang = "vi"): Promise<string> {
  const res = await apiFetch("/api/session/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_name: userName, user_lang: lang }),
  });
  if (!res.ok) throw new Error(`session_start_${res.status}`);
  const data = await res.json();
  return data.session_id as string;
}

export async function sendChat(
  question: string,
  department: string,
  sessionId: string,
): Promise<ChatResponse | { __sessionExpired: true }> {
  const res = await apiFetch("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, department, session_id: sessionId }),
  });
  if (res.status === 404) return { __sessionExpired: true };
  if (!res.ok) throw new Error(`chat_${res.status}`);
  return (await res.json()) as ChatResponse;
}
