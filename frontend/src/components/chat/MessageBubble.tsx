import { marked } from "marked";
import { FileText, Search, User, Waves } from "lucide-react";
import { cn } from "@/lib/utils";

export interface ChatMessage {
  id: string;
  role: "user" | "bot";
  text: string;
  sources?: string[];
  rewritten?: string;
}

interface MessageBubbleProps {
  message: ChatMessage;
}

export function MessageBubble({ message }: MessageBubbleProps) {
  const isUser = message.role === "user";

  return (
    <div
      className={cn(
        "flex w-full animate-fade-in gap-2.5 sm:gap-3",
        isUser ? "justify-end" : "justify-start",
      )}
    >
      {!isUser && (
        <div className="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-2xl bg-gradient-ocean text-primary-foreground shadow-soft">
          <Waves className="h-[18px] w-[18px]" strokeWidth={2.2} />
        </div>
      )}

      <div
        className={cn(
          "flex max-w-[85%] flex-col gap-2 sm:max-w-[75%]",
          isUser ? "items-end" : "items-start",
        )}
      >
        <div
          className={cn(
            "rounded-2xl px-4 py-3 text-sm leading-relaxed shadow-soft sm:px-5 sm:py-3.5",
            isUser
              ? "rounded-tr-md bg-gradient-ocean text-primary-foreground"
              : "rounded-tl-md border border-border bg-card text-foreground",
          )}
        >
          {isUser ? (
            <p className="whitespace-pre-wrap break-words">{message.text}</p>
          ) : (
            <div
              className="prose-bubble break-words"
              dangerouslySetInnerHTML={{ __html: marked.parse(message.text) as string }}
            />
          )}
        </div>

        {!isUser && message.rewritten && message.rewritten !== message.text && (
          <div className="flex items-center gap-1.5 px-1 text-[11px] font-medium text-muted-foreground">
            <Search className="h-3 w-3" />
            <span className="truncate">Đã tìm: "{message.rewritten}"</span>
          </div>
        )}

        {!isUser && message.sources && message.sources.length > 0 && (
          <div className="flex flex-wrap gap-1.5 px-1">
            {message.sources.map((src) => (
              <span
                key={src}
                className="inline-flex items-center gap-1 rounded-full border border-border bg-secondary px-2.5 py-0.5 text-[11px] font-medium text-secondary-foreground"
              >
                <FileText className="h-3 w-3" />
                {src}
              </span>
            ))}
          </div>
        )}
      </div>

      {isUser && (
        <div className="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-2xl border border-border bg-card text-ocean-deep shadow-soft">
          <User className="h-[18px] w-[18px]" strokeWidth={2.2} />
        </div>
      )}
    </div>
  );
}

export function TypingBubble() {
  return (
    <div className="flex animate-fade-in gap-2.5 sm:gap-3">
      <div className="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-2xl bg-gradient-ocean text-primary-foreground shadow-soft">
        <Waves className="h-[18px] w-[18px]" strokeWidth={2.2} />
      </div>
      <div className="flex items-center gap-1.5 rounded-2xl rounded-tl-md border border-border bg-card px-5 py-4 shadow-soft">
        {[0, 0.15, 0.3].map((delay) => (
          <span
            key={delay}
            className="h-2 w-2 rounded-full bg-ocean"
            style={{ animation: `typing-bounce 1.2s ease-in-out ${delay}s infinite` }}
          />
        ))}
      </div>
    </div>
  );
}
