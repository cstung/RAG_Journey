import { cn } from "@/lib/utils";

interface LogoProps {
  className?: string;
  size?: number;
  showWordmark?: boolean;
}

/**
 * Stylised "joyful wave" mark inspired by the Lotte World Aquarium Hanoi
 * brand framework — flowing strokes that read as both wave and fin.
 */
export function Logo({ className, size = 36, showWordmark = true }: LogoProps) {
  return (
    <div className={cn("flex items-center gap-2.5", className)}>
      <svg
        width={size}
        height={size}
        viewBox="0 0 48 48"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
        className="shrink-0"
      >
        <path
          d="M4 28c6-10 14-14 22-12 6 1.5 11 6 18 4"
          stroke="hsl(var(--ocean))"
          strokeWidth="2.4"
          strokeLinecap="round"
        />
        <path
          d="M4 33c6-10 14-14 22-12 6 1.5 11 6 18 4"
          stroke="hsl(var(--ocean-glow))"
          strokeWidth="2.4"
          strokeLinecap="round"
          opacity="0.75"
        />
        <path
          d="M4 38c6-10 14-14 22-12 6 1.5 11 6 18 4"
          stroke="hsl(var(--bubble))"
          strokeWidth="2.4"
          strokeLinecap="round"
          opacity="0.55"
        />
        <circle cx="38" cy="18" r="2.2" fill="hsl(var(--coral))" />
      </svg>
      {showWordmark && (
        <div className="leading-none">
          <div className="font-display text-[15px] font-extrabold tracking-tight text-ocean-deep">
            Lotte World Aquarium
          </div>
          <div className="mt-0.5 text-[10px] font-bold uppercase tracking-[0.18em] text-coral">
            Hanoi · Internal Assistant
          </div>
        </div>
      )}
    </div>
  );
}
