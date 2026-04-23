import { LogIn, LogOut, Sparkles } from "lucide-react";
import { Logo } from "@/components/brand/Logo";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";

interface TopBarProps {
  isAdmin: boolean;
  stats: { total_files: number; total_chunks: number } | null;
  onAdminClick: () => void;
}

export function TopBar({ isAdmin, stats, onAdminClick }: TopBarProps) {
  return (
    <header className="sticky top-0 z-30 border-b border-border bg-card/80 backdrop-blur-md">
      <div className="container flex h-16 items-center justify-between gap-4">
        <Logo />

        <div className="flex items-center gap-2 sm:gap-3">
          {isAdmin && stats && (
            <Badge
              variant="secondary"
              className="hidden sm:inline-flex gap-1.5 rounded-full border border-border bg-secondary px-3 py-1 text-xs font-semibold text-ocean-deep"
            >
              <Sparkles className="h-3.5 w-3.5 text-coral" />
              {stats.total_files} files · {stats.total_chunks} chunks
            </Badge>
          )}
          <Button
            variant={isAdmin ? "outline" : "ghost"}
            size="sm"
            onClick={onAdminClick}
            className="gap-1.5 rounded-full"
            aria-label={isAdmin ? "Logout" : "Admin"}
          >
            {isAdmin ? (
              <>
                <LogOut className="h-4 w-4" />
                <span className="hidden sm:inline">Logout</span>
              </>
            ) : (
              <>
                <LogIn className="h-4 w-4" />
                <span className="hidden sm:inline">Admin</span>
              </>
            )}
          </Button>
        </div>
      </div>
    </header>
  );
}
