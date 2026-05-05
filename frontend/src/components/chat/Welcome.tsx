import { Waves, MessageCircleQuestion, Sparkles, BookOpen } from "lucide-react";

interface WelcomeProps {
  onSuggest: (q: string) => void;
}

const suggestions = [
  { icon: BookOpen, text: "Quy trình xin nghỉ phép như thế nào?" },
  { icon: Sparkles, text: "Chính sách thưởng cuối năm ra sao?" },
  { icon: MessageCircleQuestion, text: "Quy định về giờ làm việc?" },
];

export function Welcome({ onSuggest }: WelcomeProps) {
  return (
    <div className="mx-auto flex max-w-2xl flex-col items-center px-4 py-12 text-center sm:py-16">
      <div className="relative mb-6 flex h-20 w-20 items-center justify-center rounded-3xl bg-gradient-ocean shadow-float">
        <Waves className="h-10 w-10 text-primary-foreground" strokeWidth={2.2} />
        <span className="absolute -right-1 -top-1 h-3 w-3 rounded-full bg-coral animate-bubble" />
        <span
          className="absolute -bottom-1 -left-1 h-2 w-2 rounded-full bg-sun animate-bubble"
          style={{ animationDelay: "1s" }}
        />
      </div>

      <h2 className="font-display text-2xl font-extrabold tracking-tight text-ocean-deep sm:text-3xl">
        Trợ lý pháp lý nội bộ
      </h2>
      <p className="mt-2 max-w-md text-sm text-muted-foreground sm:text-base">
        Hỏi tôi bất cứ điều gì về quy định, quy trình hoặc chính sách nội bộ.
        Tôi sẽ tìm trong tài liệu và trả lời cho bạn.
      </p>

      <div className="mt-8 grid w-full gap-2.5 sm:grid-cols-3 sm:gap-3">
        {suggestions.map(({ icon: Icon, text }) => (
          <button
            key={text}
            onClick={() => onSuggest(text)}
            className="group flex items-start gap-2.5 rounded-2xl border border-border bg-card/70 p-3.5 text-left text-sm text-foreground shadow-soft transition-all hover:-translate-y-0.5 hover:border-ocean/40 hover:bg-card hover:shadow-float focus-ring"
          >
            <span className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-secondary text-ocean transition-colors group-hover:bg-ocean group-hover:text-primary-foreground">
              <Icon className="h-4 w-4" />
            </span>
            <span className="leading-snug">{text}</span>
          </button>
        ))}
      </div>
    </div>
  );
}
