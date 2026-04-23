import { useCallback, useEffect, useRef, useState } from "react";
import { TopBar } from "@/components/chat/TopBar";
import { ChatInput } from "@/components/chat/ChatInput";
import { Welcome } from "@/components/chat/Welcome";
import {
  ChatMessage,
  MessageBubble,
  TypingBubble,
} from "@/components/chat/MessageBubble";
import { UserNameDialog } from "@/components/chat/UserNameDialog";
import { AdminLoginDialog } from "@/components/admin/AdminLoginDialog";
import { AdminPanel } from "@/components/admin/AdminPanel";
import { fetchStats, sendChat, startSession, Stats } from "@/lib/api";
import { toast } from "@/hooks/use-toast";

const ADMIN_KEY = "admin_token";
const NAME_KEY = "user_name";
const SESSION_KEY = "session_id";

const Index = () => {
  // ── Auth / identity state ─────────────────────────────────────────────
  const [adminToken, setAdminToken] = useState<string | null>(
    () => localStorage.getItem(ADMIN_KEY),
  );
  const [adminOpen, setAdminOpen] = useState(false);
  const [userName, setUserName] = useState<string>(
    () => localStorage.getItem(NAME_KEY) || "",
  );
  const [nameDialogOpen, setNameDialogOpen] = useState(false);
  const sessionIdRef = useRef<string>(localStorage.getItem(SESSION_KEY) || "");

  // ── Chat state ────────────────────────────────────────────────────────
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [typing, setTyping] = useState(false);
  const [department, setDepartment] = useState("all");
  const scrollRef = useRef<HTMLDivElement>(null);

  // ── Stats / departments ───────────────────────────────────────────────
  const [stats, setStats] = useState<Stats | null>(null);

  const refreshStats = useCallback(async () => {
    const s = await fetchStats(adminToken);
    if (s) setStats(s);
  }, [adminToken]);

  useEffect(() => {
    refreshStats();
    const t = setInterval(refreshStats, 30000);
    return () => clearInterval(t);
  }, [refreshStats]);

  // Auto-scroll
  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [messages, typing]);

  // ── Session bootstrap ─────────────────────────────────────────────────
  const ensureSession = useCallback(async (): Promise<string | null> => {
    if (!userName) {
      setNameDialogOpen(true);
      return null;
    }
    if (sessionIdRef.current) return sessionIdRef.current;
    try {
      const id = await startSession(userName, "vi");
      sessionIdRef.current = id;
      localStorage.setItem(SESSION_KEY, id);
      return id;
    } catch {
      toast({
        title: "Không tạo được phiên làm việc",
        description: "Vui lòng thử lại sau.",
        variant: "destructive",
      });
      return null;
    }
  }, [userName]);

  // ── Send a message ────────────────────────────────────────────────────
  const handleSend = useCallback(
    async (text: string) => {
      const userMsg: ChatMessage = {
        id: `${Date.now()}-u`,
        role: "user",
        text,
      };
      setMessages((m) => [...m, userMsg]);
      setTyping(true);

      const sid = await ensureSession();
      if (!sid) {
        setTyping(false);
        return;
      }

      try {
        const res = await sendChat(text, department, sid);
        if ("__sessionExpired" in res) {
          localStorage.removeItem(SESSION_KEY);
          sessionIdRef.current = "";
          setMessages((m) => [
            ...m,
            {
              id: `${Date.now()}-b`,
              role: "bot",
              text: "Phiên làm việc đã hết hạn. Vui lòng gửi lại câu hỏi.",
            },
          ]);
        } else {
          setMessages((m) => [
            ...m,
            {
              id: `${Date.now()}-b`,
              role: "bot",
              text: res.answer,
              sources: res.sources,
              rewritten: res.rewritten_query,
            },
          ]);
        }
      } catch (err) {
        setMessages((m) => [
          ...m,
          {
            id: `${Date.now()}-b`,
            role: "bot",
            text: `Lỗi kết nối: ${(err as Error).message}`,
          },
        ]);
      } finally {
        setTyping(false);
      }
    },
    [department, ensureSession],
  );

  // ── Admin handlers ────────────────────────────────────────────────────
  function handleAdminClick() {
    if (adminToken) {
      localStorage.removeItem(ADMIN_KEY);
      setAdminToken(null);
      toast({ title: "Đã đăng xuất" });
    } else {
      setAdminOpen(true);
    }
  }

  function handleAdminSuccess(token: string) {
    localStorage.setItem(ADMIN_KEY, token);
    setAdminToken(token);
    setAdminOpen(false);
    toast({ title: "Chào mừng trở lại, admin." });
  }

  function handleTokenInvalid() {
    localStorage.removeItem(ADMIN_KEY);
    setAdminToken(null);
    toast({
      title: "Phiên admin đã hết hạn",
      description: "Vui lòng đăng nhập lại.",
      variant: "destructive",
    });
  }

  function handleNameConfirm(name: string) {
    setUserName(name);
    localStorage.setItem(NAME_KEY, name);
    setNameDialogOpen(false);
  }

  const departments = ["all", ...(stats?.departments ?? [])];

  return (
    <div className="flex h-dvh flex-col">
      <TopBar
        isAdmin={!!adminToken}
        stats={stats}
        onAdminClick={handleAdminClick}
      />

      {adminToken && (
        <AdminPanel
          token={adminToken}
          departments={stats?.departments ?? []}
          onStatsRefresh={refreshStats}
          onTokenInvalid={handleTokenInvalid}
        />
      )}

      <main
        ref={scrollRef}
        className="flex-1 overflow-y-auto scrollbar-thin"
      >
        <div className="container py-6 sm:py-8">
          {messages.length === 0 ? (
            <Welcome onSuggest={(q) => handleSend(q)} />
          ) : (
            <div className="mx-auto flex max-w-3xl flex-col gap-5">
              {messages.map((m) => (
                <MessageBubble key={m.id} message={m} />
              ))}
              {typing && <TypingBubble />}
            </div>
          )}
        </div>
      </main>

      <ChatInput
        departments={departments}
        department={department}
        onDepartmentChange={setDepartment}
        onSend={handleSend}
        disabled={typing}
      />

      <AdminLoginDialog
        open={adminOpen}
        onClose={() => setAdminOpen(false)}
        onSuccess={handleAdminSuccess}
      />
      <UserNameDialog open={nameDialogOpen} onConfirm={handleNameConfirm} />
    </div>
  );
};

export default Index;
