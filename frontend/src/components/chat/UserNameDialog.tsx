import { useEffect, useRef, useState } from "react";
import { Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

interface UserNameDialogProps {
  open: boolean;
  onConfirm: (name: string) => void;
}

/** Branded replacement for the original window.prompt() flow. */
export function UserNameDialog({ open, onConfirm }: UserNameDialogProps) {
  const [name, setName] = useState("");
  const ref = useRef<HTMLInputElement>(null);
  const titleId = "user-name-dialog-title";
  const descId = "user-name-dialog-desc";

  useEffect(() => {
    if (open) setTimeout(() => ref.current?.focus(), 50);
  }, [open]);

  useEffect(() => {
    if (!open) return;

    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        e.preventDefault();
        onConfirm(name.trim() || "Guest");
      }
    }

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, name, onConfirm]);

  if (!open) return null;

  function submit(e: React.FormEvent) {
    e.preventDefault();
    onConfirm(name.trim() || "Guest");
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-ink/50 p-4 backdrop-blur-sm animate-fade-in" role="dialog" aria-modal="true" aria-labelledby={titleId} aria-describedby={descId}>
      <form
        onSubmit={submit}
        className="w-full max-w-sm overflow-hidden rounded-3xl border border-border bg-card shadow-float"
      >
        <div className="bg-gradient-ocean p-6 text-primary-foreground">
          <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-white/15">
            <Sparkles className="h-5 w-5" />
          </div>
          <h3 id={titleId} className="mt-3 font-display text-lg font-extrabold">Xin chào!</h3>
          <p id={descId} className="mt-0.5 text-sm text-primary-foreground/85">
            Cho tôi biết tên bạn để bắt đầu cuộc trò chuyện.
          </p>
        </div>
        <div className="space-y-4 p-6">
          <div className="space-y-1.5">
            <Label htmlFor="user-name">Tên của bạn</Label>
            <Input
              id="user-name"
              ref={ref}
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Ví dụ: Minh Anh"
            />
          </div>
          <Button type="submit" className="w-full bg-coral hover:bg-coral/90">
            Bắt đầu
          </Button>
        </div>
      </form>
    </div>
  );
}
