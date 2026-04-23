import { useEffect, useRef, useState } from "react";
import { Lock, X } from "lucide-react";
import { apiFetch } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

interface AdminLoginDialogProps {
  open: boolean;
  onClose: () => void;
  onSuccess: (token: string) => void;
}

export function AdminLoginDialog({ open, onClose, onSuccess }: AdminLoginDialogProps) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const userRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open) {
      setError(null);
      setTimeout(() => userRef.current?.focus(), 50);
    }
  }, [open]);

  useEffect(() => {
    if (!open) return;

    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        e.preventDefault();
        onClose();
      }
    }

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, onClose]);

  if (!open) return null;

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const res = await apiFetch("/api/admin/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });
      if (res.status === 401) {
        setError("Sai tài khoản hoặc mật khẩu.");
        return;
      }
      if (!res.ok) {
        setError(`Lỗi đăng nhập (${res.status}).`);
        return;
      }
      const data = await res.json();
      if (!data.token) {
        setError("Không nhận được token.");
        return;
      }
      onSuccess(data.token);
    } catch {
      setError("Không kết nối được máy chủ.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-ink/50 p-4 backdrop-blur-sm animate-fade-in"
      onClick={onClose}
      role="dialog"
      aria-modal="true"
    >
      <form
        onSubmit={submit}
        onClick={(e) => e.stopPropagation()}
        className="w-full max-w-sm overflow-hidden rounded-3xl border border-border bg-card shadow-float"
      >
        <div className="relative bg-gradient-ocean p-6 text-primary-foreground">
          <button
            type="button"
            onClick={onClose}
            className="absolute right-3 top-3 flex h-8 w-8 items-center justify-center rounded-full text-primary-foreground/80 transition-colors hover:bg-white/15 hover:text-primary-foreground"
            aria-label="Đóng"
          >
            <X className="h-4 w-4" />
          </button>
          <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-white/15">
            <Lock className="h-5 w-5" />
          </div>
          <h3 className="mt-3 font-display text-lg font-extrabold">Admin sign-in</h3>
          <p className="mt-0.5 text-sm text-primary-foreground/85">
            Truy cập quản trị nội bộ.
          </p>
        </div>

        <div className="space-y-4 p-6">
          <div className="space-y-1.5">
            <Label htmlFor="admin-user">Tài khoản</Label>
            <Input
              id="admin-user"
              ref={userRef}
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoComplete="username"
              required
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="admin-pass">Mật khẩu</Label>
            <Input
              id="admin-pass"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete="current-password"
              required
            />
          </div>
          {error && (
            <p className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs font-medium text-destructive">
              {error}
            </p>
          )}
          <Button
            type="submit"
            disabled={loading}
            className="w-full bg-coral text-primary-foreground hover:bg-coral/90"
          >
            {loading ? "Đang đăng nhập…" : "Đăng nhập"}
          </Button>
        </div>
      </form>
    </div>
  );
}
