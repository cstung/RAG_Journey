import { KeyboardEvent, useRef, useState } from "react";
import { Building2, SendHorizontal } from "lucide-react";
import { cn } from "@/lib/utils";

interface ChatInputProps {
  departments: string[];
  department: string;
  onDepartmentChange: (d: string) => void;
  onSend: (text: string) => void;
  disabled?: boolean;
}

export function ChatInput({
  departments,
  department,
  onDepartmentChange,
  onSend,
  disabled,
}: ChatInputProps) {
  const [value, setValue] = useState("");
  const ref = useRef<HTMLTextAreaElement>(null);

  function autosize() {
    const el = ref.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, 160) + "px";
  }

  function handleSend() {
    const trimmed = value.trim();
    if (!trimmed || disabled) return;
    onSend(trimmed);
    setValue("");
    requestAnimationFrame(autosize);
  }

  function handleKey(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  }

  return (
    <div className="border-t border-border bg-card/85 backdrop-blur-md">
      <div className="container py-3 sm:py-4">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:gap-3">
          <label className="relative flex-shrink-0">
            <span className="sr-only">Phòng ban</span>
            <Building2 className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-ocean" />
            <select
              value={department}
              onChange={(e) => onDepartmentChange(e.target.value)}
              className="h-11 w-full appearance-none rounded-xl border border-border bg-secondary pl-9 pr-8 text-sm font-semibold text-secondary-foreground shadow-soft transition-colors hover:bg-bubble/40 focus-ring sm:w-auto"
            >
              {departments.map((d) => (
                <option key={d} value={d}>
                  {d === "all" ? "Tất cả phòng ban" : d}
                </option>
              ))}
            </select>
          </label>

          <div className="flex flex-1 items-end gap-2 rounded-2xl border border-border bg-background px-3 py-2 shadow-soft transition-shadow focus-within:shadow-float">
            <textarea
              ref={ref}
              rows={1}
              value={value}
              onChange={(e) => {
                setValue(e.target.value);
                autosize();
              }}
              onKeyDown={handleKey}
              placeholder="Nhập câu hỏi của bạn… (Enter để gửi · Shift+Enter xuống dòng)"
              className="max-h-40 min-h-[28px] flex-1 resize-none border-0 bg-transparent px-1 py-1.5 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none"
              disabled={disabled}
            />
            <button
              type="button"
              onClick={handleSend}
              disabled={disabled || !value.trim()}
              aria-label="Gửi câu hỏi"
              className={cn(
                "flex h-9 w-9 shrink-0 items-center justify-center rounded-xl transition-all focus-ring",
                value.trim() && !disabled
                  ? "bg-coral text-primary-foreground shadow-soft hover:-translate-y-0.5 hover:shadow-float"
                  : "bg-muted text-muted-foreground",
              )}
            >
              <SendHorizontal className="h-[18px] w-[18px]" />
            </button>
          </div>
        </div>
        <p className="mt-2 hidden text-center text-[11px] text-muted-foreground sm:block">
          Where wonder moves you · Built to work beyond the board.
        </p>
      </div>
    </div>
  );
}
