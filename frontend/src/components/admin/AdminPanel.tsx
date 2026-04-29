import { useCallback, useEffect, useRef, useState } from "react";
import {
  BarChart3,
  Bell,
  ChevronLeft,
  ChevronRight,
  FileStack,
  FolderOpen,
  Globe,
  Loader2,
  Mail,
  MessageSquare,
  RefreshCw,
  Trash2,
  Upload,
  Database,
} from "lucide-react";
import { adminFetch } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

type Tab = "dashboard" | "conversations" | "negative" | "documents" | "notifications" | "datasets";

const TABS: { id: Tab; label: string; icon: typeof BarChart3 }[] = [
  { id: "dashboard", label: "Dashboard", icon: BarChart3 },
  { id: "conversations", label: "Conversations", icon: MessageSquare },
  { id: "negative", label: "Negative", icon: Bell },
  { id: "documents", label: "Documents", icon: FolderOpen },
  { id: "notifications", label: "Emails", icon: Mail },
  { id: "datasets", label: "Datasets", icon: Database },
];

interface AdminPanelProps {
  token: string;
  departments: string[];
  onStatsRefresh: () => void;
  onTokenInvalid: () => void;
}

export function AdminPanel({ token, departments, onStatsRefresh, onTokenInvalid }: AdminPanelProps) {
  const [tab, setTab] = useState<Tab>(
    (localStorage.getItem("admin_tab") as Tab) || "dashboard",
  );
  const [status, setStatus] = useState<{ kind: "info" | "ok" | "err"; text: string } | null>(null);
  const [dept, setDept] = useState("General");
  const fileRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    localStorage.setItem("admin_tab", tab);
  }, [tab]);

  const guard = useCallback(
    async (path: string, init?: RequestInit) => {
      const res = await adminFetch(path, token, init);
      if (res.status === 401) {
        onTokenInvalid();
        throw new Error("unauthorized");
      }
      return res;
    },
    [token, onTokenInvalid],
  );

  async function uploadFiles(files: FileList | null) {
    if (!files || !files.length) return;
    for (const file of Array.from(files)) {
      setStatus({ kind: "info", text: `Đang tải ${file.name} → [${dept}]…` });
      const formData = new FormData();
      formData.append("file", file);
      try {
        const res = await guard(
          `/api/admin/upload?department=${encodeURIComponent(dept)}`,
          { method: "POST", body: formData },
        );
        const data = await res.json();
        setStatus({
          kind: res.ok ? "ok" : "err",
          text: data.message || `HTTP ${res.status}`,
        });
      } catch (err) {
        setStatus({ kind: "err", text: (err as Error).message });
      }
    }
    if (fileRef.current) fileRef.current.value = "";
    onStatsRefresh();
  }

  async function ingestAll() {
    setStatus({ kind: "info", text: "Đang index lại toàn bộ tài liệu…" });
    try {
      const res = await guard("/api/admin/ingest-all", { method: "POST" });
      const data = await res.json();
      setStatus({ kind: "ok", text: data.message || "Done" });
      onStatsRefresh();
    } catch (err) {
      setStatus({ kind: "err", text: (err as Error).message });
    }
  }

  async function resetDB() {
    if (!confirm("Bạn có chắc chắn muốn xóa sạch database?")) return;
    setStatus({ kind: "info", text: "Đang xóa database…" });
    try {
      const res = await guard("/api/admin/reset", { method: "POST" });
      const data = await res.json();
      setStatus({ kind: "ok", text: data.message || "Reset" });
      onStatsRefresh();
    } catch (err) {
      setStatus({ kind: "err", text: (err as Error).message });
    }
  }

  return (
    <section className="border-b border-border bg-card/70 backdrop-blur-md">
      <div className="container py-4 sm:py-6">
        {/* Toolbar */}
        <div className="flex flex-col gap-3 rounded-2xl border border-border bg-background/60 p-3 shadow-soft sm:flex-row sm:items-center sm:gap-3 sm:p-4">
          <div className="flex flex-1 flex-wrap items-center gap-2">
            <span className="inline-flex items-center gap-1.5 text-xs font-bold uppercase tracking-wider text-ocean-deep">
              <Upload className="h-3.5 w-3.5" /> Upload PDF
            </span>
            <select
              value={dept}
              onChange={(e) => setDept(e.target.value)}
              className="h-9 rounded-lg border border-border bg-card px-3 text-xs font-semibold text-foreground shadow-soft focus-ring"
            >
              {Array.from(
                new Set(["General", "HR", "IT", "Finance", "Operations", "Marketing", ...departments]),
              )
                .sort()
                .map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
            </select>
            <input
              type="file"
              accept=".pdf"
              multiple
              ref={fileRef}
              className="hidden"
              onChange={(e) => uploadFiles(e.target.files)}
            />
            <Button size="sm" variant="default" onClick={() => fileRef.current?.click()}>
              <Upload className="h-4 w-4" /> Chọn file
            </Button>
            <Button size="sm" variant="outline" onClick={ingestAll}>
              <RefreshCw className="h-4 w-4" /> Index lại
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={resetDB}
              className="border-destructive/30 text-destructive hover:bg-destructive/10 hover:text-destructive"
            >
              <Trash2 className="h-4 w-4" /> Xóa DB
            </Button>
          </div>
          {status && (
            <p
              className={cn(
                "truncate text-xs font-medium",
                status.kind === "ok" && "text-[hsl(var(--success))]",
                status.kind === "err" && "text-destructive",
                status.kind === "info" && "text-muted-foreground",
              )}
            >
              {status.text}
            </p>
          )}
        </div>

        {/* Tabs */}
        <div className="mt-4 flex gap-1.5 overflow-x-auto pb-1 scrollbar-thin">
          {TABS.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => setTab(id)}
              className={cn(
                "inline-flex shrink-0 items-center gap-1.5 rounded-full border px-3.5 py-2 text-xs font-semibold transition-all focus-ring",
                tab === id
                  ? "border-transparent bg-gradient-ocean text-primary-foreground shadow-soft"
                  : "border-border bg-card text-foreground hover:border-ocean/40 hover:text-ocean-deep",
              )}
            >
              <Icon className="h-3.5 w-3.5" />
              {label}
            </button>
          ))}
        </div>

        {/* Tab content */}
        <div className="mt-4">
          {tab === "dashboard" && <DashboardTab guard={guard} />}
          {tab === "conversations" && <SessionsTab guard={guard} />}
          {tab === "negative" && <NegativeTab guard={guard} />}
          {tab === "documents" && (
            <DocumentsTab guard={guard} setStatus={setStatus} onStatsRefresh={onStatsRefresh} />
          )}
          {tab === "notifications" && <EmailsTab guard={guard} setStatus={setStatus} />}
          {tab === "datasets" && <DatasetsTab guard={guard} setStatus={setStatus} />}
        </div>
      </div>
    </section>
  );
}

/* ─────────────── Reusable card / table chrome ─────────────── */

function Card({
  title,
  icon: Icon,
  children,
}: {
  title: string;
  icon: typeof BarChart3;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-2xl border border-border bg-card p-4 shadow-soft sm:p-5">
      <div className="mb-3 flex items-center gap-2 text-sm font-bold text-ocean-deep">
        <Icon className="h-4 w-4 text-coral" />
        {title}
      </div>
      {children}
    </div>
  );
}

function Pager({
  page,
  total,
  pageSize,
  onPrev,
  onNext,
}: {
  page: number;
  total: number;
  pageSize: number;
  onPrev: () => void;
  onNext: () => void;
}) {
  const maxPage = Math.max(1, Math.ceil((total || 0) / pageSize));
  return (
    <div className="mt-3 flex items-center justify-end gap-2 text-xs text-muted-foreground">
      <Button size="sm" variant="outline" onClick={onPrev} disabled={page <= 1}>
        <ChevronLeft className="h-3.5 w-3.5" />
      </Button>
      <span className="font-semibold">
        Page {page} / {maxPage} · {total || 0}
      </span>
      <Button size="sm" variant="outline" onClick={onNext} disabled={page >= maxPage}>
        <ChevronRight className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}

function TableShell({ children }: { children: React.ReactNode }) {
  return (
    <div className="overflow-x-auto rounded-xl border border-border scrollbar-thin">
      <table className="w-full text-left text-xs sm:text-sm">{children}</table>
    </div>
  );
}

const TH = ({ children }: { children: React.ReactNode }) => (
  <th className="border-b border-border bg-muted/60 px-3 py-2 font-semibold text-ocean-deep">
    {children}
  </th>
);
const TD = ({ children, className }: { children: React.ReactNode; className?: string }) => (
  <td className={cn("border-b border-border/60 px-3 py-2 align-top", className)}>{children}</td>
);

/* ─────────────── Tabs ─────────────── */

type GuardFn = (path: string, init?: RequestInit) => Promise<Response>;

function DashboardTab({ guard }: { guard: GuardFn }) {
  const [stats, setStats] = useState<any | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    guard("/api/admin/stats")
      .then((r) => r.json())
      .then((d) => alive && setStats(d))
      .catch(() => alive && setStats(null))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [guard]);

  return (
    <div className="grid gap-4 sm:grid-cols-3">
      <StatCard
        label="Files"
        value={loading ? "…" : stats?.total_files ?? "—"}
        icon={FileStack}
      />
      <StatCard
        label="Chunks"
        value={loading ? "…" : stats?.total_chunks ?? "—"}
        icon={BarChart3}
      />
      <StatCard
        label="Departments"
        value={loading ? "…" : (stats?.departments?.length ?? 0)}
        icon={FolderOpen}
      />
    </div>
  );
}

function StatCard({
  label,
  value,
  icon: Icon,
}: {
  label: string;
  value: number | string;
  icon: typeof BarChart3;
}) {
  return (
    <div className="rounded-2xl border border-border bg-card p-5 shadow-soft">
      <div className="flex items-center justify-between">
        <span className="text-xs font-bold uppercase tracking-wider text-muted-foreground">
          {label}
        </span>
        <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-secondary text-ocean">
          <Icon className="h-4 w-4" />
        </span>
      </div>
      <div className="mt-3 font-display text-3xl font-extrabold text-ocean-deep">{value}</div>
    </div>
  );
}

const PAGE_SIZE = 20;

function SessionsTab({ guard }: { guard: GuardFn }) {
  const [items, setItems] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [name, setName] = useState("");
  const [statusF, setStatusF] = useState("all");
  const [loading, setLoading] = useState(false);
  const [detail, setDetail] = useState<any | null>(null);

  const load = useCallback(
    async (p = 1) => {
      setLoading(true);
      setPage(p);
      const qs = new URLSearchParams({
        page: String(p),
        page_size: String(PAGE_SIZE),
        status: statusF,
      });
      if (name.trim()) qs.set("user_name", name.trim());
      try {
        const res = await guard(`/api/admin/sessions?${qs}`);
        const data = await res.json();
        setItems(data.items || []);
        setTotal(data.total || 0);
      } finally {
        setLoading(false);
      }
    },
    [guard, name, statusF],
  );

  useEffect(() => {
    load(1);
  }, [load]);

  async function openDetail(id: string) {
    const res = await guard(`/api/admin/sessions/${encodeURIComponent(id)}`);
    setDetail(await res.json());
  }

  return (
    <Card title="Sessions" icon={MessageSquare}>
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <Input
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="Filter user name…"
          className="h-9 w-full sm:w-56"
        />
        <select
          value={statusF}
          onChange={(e) => setStatusF(e.target.value)}
          className="h-9 rounded-lg border border-border bg-card px-3 text-xs font-semibold focus-ring"
        >
          <option value="all">All</option>
          <option value="active">Active</option>
          <option value="ended">Ended</option>
        </select>
        <Button size="sm" variant="outline" onClick={() => load(1)}>
          Search
        </Button>
      </div>

      {loading ? (
        <Loading />
      ) : !items.length ? (
        <Empty label="No sessions" />
      ) : (
        <TableShell>
          <thead>
            <tr>
              <TH>User</TH>
              <TH>Lang</TH>
              <TH>Created</TH>
              <TH>Ended</TH>
              <TH>ID</TH>
            </tr>
          </thead>
          <tbody>
            {items.map((s) => (
              <tr
                key={s.id}
                onClick={() => openDetail(s.id)}
                className="cursor-pointer transition-colors hover:bg-secondary/60"
              >
                <TD>{s.user_name}</TD>
                <TD>{s.user_lang}</TD>
                <TD>{s.created_at}</TD>
                <TD>{s.ended_at || "—"}</TD>
                <TD className="font-mono text-[11px]">{s.id}</TD>
              </tr>
            ))}
          </tbody>
        </TableShell>
      )}

      <Pager
        page={page}
        total={total}
        pageSize={PAGE_SIZE}
        onPrev={() => load(page - 1)}
        onNext={() => load(page + 1)}
      />

      <DetailModal title="Session detail" data={detail} onClose={() => setDetail(null)}>
        {detail && (
          <>
            <DetailHeader
              rows={[
                ["User", `${detail.user_name} (${detail.user_lang})`],
                ["Created", detail.created_at],
                ["Ended", detail.ended_at || "—"],
                ["Session", detail.id],
              ]}
            />
            <PreBlock title={`Messages (${(detail.messages || []).length})`}>
              {(detail.messages || []).map((m: any) => `${m.role}: ${m.content}`).join("\n\n")}
            </PreBlock>
            <PreBlock title={`Feedback (${(detail.feedback || []).length})`}>
              {(detail.feedback || [])
                .map((f: any) => `rating=${f.rating} message_id=${f.message_id} reason=${f.reason || ""}`)
                .join("\n")}
            </PreBlock>
          </>
        )}
      </DetailModal>
    </Card>
  );
}

function NegativeTab({ guard }: { guard: GuardFn }) {
  const [items, setItems] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [name, setName] = useState("");
  const [sess, setSess] = useState("");
  const [loading, setLoading] = useState(false);

  const load = useCallback(
    async (p = 1) => {
      setLoading(true);
      setPage(p);
      const qs = new URLSearchParams({
        page: String(p),
        page_size: String(PAGE_SIZE),
        threshold: "-1",
      });
      if (name.trim()) qs.set("user_name", name.trim());
      if (sess.trim()) qs.set("session_id", sess.trim());
      try {
        const res = await guard(`/api/admin/feedback/negative?${qs}`);
        const data = await res.json();
        setItems(data.items || []);
        setTotal(data.total || 0);
      } finally {
        setLoading(false);
      }
    },
    [guard, name, sess],
  );

  useEffect(() => {
    load(1);
  }, [load]);

  return (
    <Card title="Negative feedback" icon={Bell}>
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <Input
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="User name…"
          className="h-9 w-full sm:w-48"
        />
        <Input
          value={sess}
          onChange={(e) => setSess(e.target.value)}
          placeholder="Session id…"
          className="h-9 w-full sm:w-56"
        />
        <Button size="sm" variant="outline" onClick={() => load(1)}>
          Search
        </Button>
      </div>
      {loading ? (
        <Loading />
      ) : !items.length ? (
        <Empty label="No negative feedback" />
      ) : (
        <TableShell>
          <thead>
            <tr>
              <TH>Rating</TH>
              <TH>User</TH>
              <TH>Reason</TH>
              <TH>Session</TH>
              <TH>Message</TH>
              <TH>At</TH>
            </tr>
          </thead>
          <tbody>
            {items.map((f, i) => (
              <tr key={i} className="hover:bg-secondary/60">
                <TD>{f.rating}</TD>
                <TD>
                  {f.user_name} ({f.user_lang})
                </TD>
                <TD>{f.reason || ""}</TD>
                <TD className="font-mono text-[11px]">{f.session_id}</TD>
                <TD>{(f.message_content || "").slice(0, 160)}</TD>
                <TD>{f.created_at}</TD>
              </tr>
            ))}
          </tbody>
        </TableShell>
      )}
      <Pager
        page={page}
        total={total}
        pageSize={PAGE_SIZE}
        onPrev={() => load(page - 1)}
        onNext={() => load(page + 1)}
      />
    </Card>
  );
}

function DocumentsTab({
  guard,
  setStatus,
  onStatsRefresh,
}: {
  guard: GuardFn;
  setStatus: (s: { kind: "info" | "ok" | "err"; text: string }) => void;
  onStatsRefresh: () => void;
}) {
  const [items, setItems] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [filename, setFilename] = useState("");
  const [active, setActive] = useState("");
  const [crawl, setCrawl] = useState("");
  const [loading, setLoading] = useState(false);
  const [detail, setDetail] = useState<any | null>(null);
  const [editDept, setEditDept] = useState("");
  const [editCat, setEditCat] = useState("");
  const [saving, setSaving] = useState(false);

  const load = useCallback(
    async (p = 1) => {
      setLoading(true);
      setPage(p);
      const qs = new URLSearchParams({ page: String(p), page_size: String(PAGE_SIZE) });
      if (filename.trim()) qs.set("filename", filename.trim());
      if (active !== "") qs.set("active", active);
      try {
        const res = await guard(`/api/admin/documents?${qs}`);
        const data = await res.json();
        setItems(data.items || []);
        setTotal(data.total || 0);
      } finally {
        setLoading(false);
      }
    },
    [guard, filename, active],
  );

  useEffect(() => {
    load(1);
  }, [load]);

  async function crawlUrl() {
    if (!crawl.trim()) return;
    setStatus({ kind: "info", text: `Crawling ${crawl}…` });
    try {
      const res = await guard("/api/admin/crawl", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: crawl.trim() }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
      setStatus({
        kind: "ok",
        text: `Crawled + indexed: ${data.file} (v${data.version}, ${data.chunks} chunks)`,
      });
      load(1);
      onStatsRefresh();
    } catch (e) {
      setStatus({ kind: "err", text: (e as Error).message });
    }
  }

  async function openDetail(id: number) {
    const res = await guard(`/api/admin/documents/${id}`);
    const data = await res.json();
    setDetail(data);
    setEditDept(data.department);
    setEditCat(data.category);
  }

  async function saveMetadata() {
    if (!detail) return;
    setSaving(true);
    setStatus({ kind: "info", text: "Updating metadata…" });
    try {
      const res = await guard(`/api/admin/documents/${detail.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          department: editDept.trim(),
          category: editCat.trim(),
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
      setStatus({ kind: "ok", text: "Metadata updated successfully" });
      setDetail(null);
      load(page);
      onStatsRefresh();
    } catch (e) {
      setStatus({ kind: "err", text: (e as Error).message });
    } finally {
      setSaving(false);
    }
  }

  return (
    <Card title="Documents" icon={FolderOpen}>
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <div className="relative flex-1 min-w-[260px]">
          <Globe className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={crawl}
            onChange={(e) => setCrawl(e.target.value)}
            placeholder="Crawl URL (https://…)"
            className="h-9 pl-9"
          />
        </div>
        <Button size="sm" variant="default" onClick={crawlUrl}>
          Crawl + Index
        </Button>
        <Input
          value={filename}
          onChange={(e) => setFilename(e.target.value)}
          placeholder="Filter filename…"
          className="h-9 w-full sm:w-48"
        />
        <select
          value={active}
          onChange={(e) => setActive(e.target.value)}
          className="h-9 rounded-lg border border-border bg-card px-3 text-xs font-semibold focus-ring"
        >
          <option value="">All</option>
          <option value="1">Active</option>
          <option value="0">Inactive</option>
        </select>
        <Button size="sm" variant="outline" onClick={() => load(1)}>
          Search
        </Button>
      </div>
      {loading ? (
        <Loading />
      ) : !items.length ? (
        <Empty label="No documents" />
      ) : (
        <TableShell>
          <thead>
            <tr>
              <TH>Filename</TH>
              <TH>Dept</TH>
              <TH>Category</TH>
              <TH>Version</TH>
              <TH>Active</TH>
              <TH>Chunks</TH>
              <TH>Uploaded</TH>
            </tr>
          </thead>
          <tbody>
            {items.map((d) => (
              <tr
                key={d.id}
                onClick={() => openDetail(d.id)}
                className="cursor-pointer hover:bg-secondary/60"
              >
                <TD>{d.filename}</TD>
                <TD>{d.department}</TD>
                <TD>{d.category}</TD>
                <TD>{d.version}</TD>
                <TD>
                  {d.is_active ? (
                    <span className="inline-flex items-center rounded-full bg-[hsl(var(--success))]/15 px-2 py-0.5 text-[10px] font-bold uppercase text-[hsl(var(--success))]">
                      Active
                    </span>
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </TD>
                <TD>{d.chunk_count}</TD>
                <TD>{d.uploaded_at}</TD>
              </tr>
            ))}
          </tbody>
        </TableShell>
      )}
      <Pager
        page={page}
        total={total}
        pageSize={PAGE_SIZE}
        onPrev={() => load(page - 1)}
        onNext={() => load(page + 1)}
      />

      <DetailModal title="Document detail" data={detail} onClose={() => setDetail(null)}>
        {detail && (
          <>
            <DetailHeader
              rows={[
                ["Filename", detail.filename],
                ["Active", detail.is_active ? "Yes" : "No"],
                ["Version", String(detail.version)],
                ["Path", detail.file_path],
              ]}
            />

            <div className="mt-4 rounded-xl border border-border bg-secondary/30 p-4">
              <h4 className="mb-3 text-[13px] font-bold text-foreground">Edit Metadata</h4>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="mb-1.5 block text-[11px] font-bold uppercase text-muted-foreground">
                    Department
                  </label>
                  <Input value={editDept} onChange={(e) => setEditDept(e.target.value)} className="h-9" />
                </div>
                <div>
                  <label className="mb-1.5 block text-[11px] font-bold uppercase text-muted-foreground">
                    Category
                  </label>
                  <Input value={editCat} onChange={(e) => setEditCat(e.target.value)} className="h-9" />
                </div>
              </div>
              <Button
                size="sm"
                className="mt-4 w-full"
                disabled={saving}
                onClick={saveMetadata}
              >
                {saving ? "Saving…" : "Save Metadata"}
              </Button>
            </div>

            <PreBlock title="Versions (max 5)">
              {(detail.versions || [])
                .map(
                  (v: any) =>
                    `v${v.version} ${v.is_active ? "[active]" : ""} chunks=${v.chunk_count} path=${v.file_path}`,
                )
                .join("\n")}
            </PreBlock>
          </>
        )}
      </DetailModal>
    </Card>
  );
}

function EmailsTab({
  guard,
  setStatus,
}: {
  guard: GuardFn;
  setStatus: (s: { kind: "info" | "ok" | "err"; text: string }) => void;
}) {
  const [items, setItems] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [statusF, setStatusF] = useState("");
  const [kind, setKind] = useState("");
  const [loading, setLoading] = useState(false);

  const load = useCallback(
    async (p = 1) => {
      setLoading(true);
      setPage(p);
      const qs = new URLSearchParams({ page: String(p), page_size: String(PAGE_SIZE) });
      if (statusF) qs.set("status", statusF);
      if (kind) qs.set("kind", kind);
      try {
        const res = await guard(`/api/admin/notifications/emails?${qs}`);
        const data = await res.json();
        setItems(data.items || []);
        setTotal(data.total || 0);
      } finally {
        setLoading(false);
      }
    },
    [guard, statusF, kind],
  );

  useEffect(() => {
    load(1);
  }, [load]);

  async function sendTest() {
    setStatus({ kind: "info", text: "Sending test email…" });
    try {
      const res = await guard("/api/admin/notifications/test-email", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
      setStatus({ kind: "ok", text: "Test email sent" });
      load(1);
    } catch (e) {
      setStatus({ kind: "err", text: (e as Error).message });
    }
  }

  return (
    <Card title="Email logs" icon={Mail}>
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <Button size="sm" variant="default" onClick={sendTest}>
          <Mail className="h-4 w-4" /> Send test
        </Button>
        <select
          value={statusF}
          onChange={(e) => setStatusF(e.target.value)}
          className="h-9 rounded-lg border border-border bg-card px-3 text-xs font-semibold focus-ring"
        >
          <option value="">All status</option>
          <option value="sent">Sent</option>
          <option value="failed">Failed</option>
        </select>
        <select
          value={kind}
          onChange={(e) => setKind(e.target.value)}
          className="h-9 rounded-lg border border-border bg-card px-3 text-xs font-semibold focus-ring"
        >
          <option value="">All kinds</option>
          <option value="negative_feedback">negative_feedback</option>
          <option value="test_email">test_email</option>
        </select>
        <Button size="sm" variant="outline" onClick={() => load(1)}>
          Refresh
        </Button>
      </div>
      {loading ? (
        <Loading />
      ) : !items.length ? (
        <Empty label="No email logs" />
      ) : (
        <TableShell>
          <thead>
            <tr>
              <TH>ID</TH>
              <TH>Kind</TH>
              <TH>Status</TH>
              <TH>To</TH>
              <TH>Subject</TH>
              <TH>Error</TH>
              <TH>At</TH>
            </tr>
          </thead>
          <tbody>
            {items.map((e) => (
              <tr key={e.id} className="hover:bg-secondary/60">
                <TD>{e.id}</TD>
                <TD>{e.kind}</TD>
                <TD>
                  <span
                    className={cn(
                      "inline-flex rounded-full px-2 py-0.5 text-[10px] font-bold uppercase",
                      e.status === "sent"
                        ? "bg-[hsl(var(--success))]/15 text-[hsl(var(--success))]"
                        : "bg-destructive/10 text-destructive",
                    )}
                  >
                    {e.status}
                  </span>
                </TD>
                <TD>{(e.to_emails || []).join(", ")}</TD>
                <TD>{e.subject}</TD>
                <TD>{(e.error || "").slice(0, 160)}</TD>
                <TD>{e.created_at}</TD>
              </tr>
            ))}
          </tbody>
        </TableShell>
      )}
      <Pager
        page={page}
        total={total}
        pageSize={PAGE_SIZE}
        onPrev={() => load(page - 1)}
        onNext={() => load(page + 1)}
      />
    </Card>
  );
}

/* ─────────────── Tiny shared bits ─────────────── */

function Loading() {
  return (
    <div className="flex items-center gap-2 py-6 text-sm text-muted-foreground">
      <Loader2 className="h-4 w-4 animate-spin" /> Loading…
    </div>
  );
}
function Empty({ label }: { label: string }) {
  return (
    <div className="rounded-xl border border-dashed border-border bg-muted/40 px-4 py-8 text-center text-sm text-muted-foreground">
      {label}
    </div>
  );
}

function DetailModal({
  title,
  data,
  onClose,
  children,
}: {
  title: string;
  data: any;
  onClose: () => void;
  children: React.ReactNode;
}) {
  if (!data) return null;
  return (
    <div
      className="fixed inset-0 z-40 flex items-center justify-center bg-ink/50 p-4 backdrop-blur-sm animate-fade-in"
      onClick={onClose}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        className="flex max-h-[85vh] w-full max-w-3xl flex-col overflow-hidden rounded-3xl border border-border bg-card shadow-float"
      >
        <div className="flex items-center justify-between border-b border-border px-5 py-3">
          <h3 className="font-display text-base font-extrabold text-ocean-deep">{title}</h3>
          <Button size="sm" variant="outline" onClick={onClose}>
            Close
          </Button>
        </div>
        <div className="space-y-3 overflow-auto px-5 py-4 scrollbar-thin">{children}</div>
      </div>
    </div>
  );
}

function DatasetsTab({
  guard,
  setStatus,
}: {
  guard: GuardFn;
  setStatus: (s: { kind: "info" | "ok" | "err"; text: string }) => void;
}) {
  const [datasets, setDatasets] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [ingestModalOpen, setIngestModalOpen] = useState(false);
  const [selectedDs, setSelectedDs] = useState("");
  const [sectors, setSectors] = useState("");
  const [minYear, setMinYear] = useState("2000");
  const [legalTypes, setLegalTypes] = useState("");
  const [maxDocs, setMaxDocs] = useState("");
  
  // Job tracking
  const [activeJobs, setActiveJobs] = useState<Record<string, any>>({});
  const activeJobsRef = useRef<Record<string, any>>({});

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await guard(`/api/admin/datasets`);
      const data = await res.json();
      setDatasets(data.datasets || []);
    } catch (e) {
      setStatus({ kind: "err", text: "Failed to load datasets" });
    } finally {
      setLoading(false);
    }
  }, [guard, setStatus]);

  useEffect(() => {
    load();
  }, [load]);

  const pollJob = useCallback((jobId: string) => {
    const interval = setInterval(async () => {
      try {
        const res = await guard(`/api/admin/datasets/status/${jobId}`);
        const state = await res.json();
        
        setActiveJobs((prev) => {
          const updated = { ...prev, [jobId]: state };
          activeJobsRef.current = updated;
          return updated;
        });

        if (state.status === "completed" || (state.status && state.status.startsWith("error"))) {
          clearInterval(interval);
          setStatus({ kind: state.status === "completed" ? "ok" : "err", text: `Job ${jobId}: ${state.status}` });
        }
      } catch (e) {
        console.error("Polling error", e);
      }
    }, 3000);
  }, [guard, setStatus]);

  const handleIngest = async () => {
    if (!selectedDs) return;
    setStatus({ kind: "info", text: `Starting ingestion for ${selectedDs}...` });
    setIngestModalOpen(false);

    const config: any = {
      dataset_id: selectedDs,
      min_year: parseInt(minYear) || 2000,
    };
    
    const sList = sectors.split(",").map(s => s.trim()).filter(Boolean);
    if (sList.length) config.sectors = sList;
    
    const lList = legalTypes.split(",").map(s => s.trim()).filter(Boolean);
    if (lList.length) config.legal_types = lList;
    
    const maxD = parseInt(maxDocs);
    if (!isNaN(maxD)) config.max_docs = maxD;

    try {
      const res = await guard("/api/admin/datasets/ingest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config)
      });
      const data = await res.json();
      if (data.job_id) {
        setActiveJobs(prev => ({ ...prev, [data.job_id]: { status: "queued", embedded: 0, total: 0 } }));
        pollJob(data.job_id);
      } else {
        throw new Error(data.detail || "Failed to start job");
      }
    } catch (e) {
      setStatus({ kind: "err", text: (e as Error).message });
    }
  };

  const openIngest = (ds: string) => {
    setSelectedDs(ds);
    setIngestModalOpen(true);
  };

  return (
    <div className="space-y-4">
      <Card title="Available Datasets" icon={Database}>
        {loading ? (
          <Loading />
        ) : !datasets.length ? (
          <Empty label="No datasets found in registry" />
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {datasets.map((ds) => (
              <div key={ds} className="flex flex-col gap-3 rounded-xl border border-border bg-secondary/30 p-4">
                <h4 className="font-bold text-sm text-ocean-deep break-all">{ds}</h4>
                <Button size="sm" onClick={() => openIngest(ds)} className="mt-auto">
                  <Upload className="w-3.5 h-3.5 mr-1" /> Ingest
                </Button>
              </div>
            ))}
          </div>
        )}
      </Card>

      {Object.keys(activeJobs).length > 0 && (
        <Card title="Active Jobs" icon={RefreshCw}>
          <TableShell>
            <thead>
              <tr>
                <TH>Job ID</TH>
                <TH>Status</TH>
                <TH>Progress</TH>
              </tr>
            </thead>
            <tbody>
              {Object.entries(activeJobs).map(([jobId, state]) => {
                const total = state.total || 0;
                const embedded = state.embedded || 0;
                const pct = total > 0 ? Math.min(100, Math.round((embedded / total) * 100)) : 0;
                return (
                  <tr key={jobId} className="hover:bg-secondary/60">
                    <TD className="font-mono text-[11px]">{jobId}</TD>
                    <TD>
                      {(() => {
                        let label = state.status || "unknown";
                        let colorClass = "bg-ocean/10 text-ocean-deep";
                        
                        if (state.status === "completed") {
                          label = "Completed";
                          colorClass = "bg-[hsl(var(--success))]/15 text-[hsl(var(--success))]";
                        } else if (state.status?.startsWith("error")) {
                          label = "Error";
                          colorClass = "bg-destructive/10 text-destructive";
                        } else if (state.status === "loading_metadata") {
                          label = "Metadata";
                          colorClass = "bg-sun/20 text-amber-600 dark:text-amber-400";
                        } else if (state.status === "loading_content") {
                          label = "Downloading";
                          colorClass = "bg-coral/20 text-coral";
                        } else if (state.status === "running") {
                          label = "Embedding";
                          colorClass = "bg-ocean/20 text-ocean-deep";
                        } else if (state.status === "queued") {
                          label = "Queued";
                          colorClass = "bg-secondary text-secondary-foreground";
                        }

                        return (
                          <span className={cn("inline-flex rounded-full px-2 py-0.5 text-[10px] font-bold uppercase whitespace-nowrap", colorClass)}>
                            {label}
                          </span>
                        );
                      })()}
                    </TD>
                    <TD>
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-semibold w-16 text-right">
                          {state.status === "loading_metadata" ? "--" : `${embedded} / ${total}`}
                        </span>
                        <div className="h-2 flex-1 rounded-full bg-border overflow-hidden relative">
                          {state.status === "loading_content" ? (
                            <div className="absolute inset-0 bg-coral/50 animate-pulse" />
                          ) : state.status === "loading_metadata" ? (
                            <div className="absolute inset-0 bg-amber-500/50 animate-pulse" />
                          ) : (
                            <div 
                              className="h-full bg-emerald-500 transition-all duration-300"
                              style={{ width: `${pct}%` }}
                            />
                          )}
                        </div>
                        <span className="text-xs font-bold w-8">
                          {state.status === "loading_metadata" || state.status === "loading_content" ? "--" : `${pct}%`}
                        </span>
                      </div>
                    </TD>
                  </tr>
                );
              })}
            </tbody>
          </TableShell>
        </Card>
      )}

      <DetailModal title={`Ingest Dataset: ${selectedDs}`} data={ingestModalOpen ? {} : null} onClose={() => setIngestModalOpen(false)}>
        <div className="space-y-4 pt-2">
          <div>
            <label className="mb-1.5 block text-[11px] font-bold uppercase text-muted-foreground">
              Sectors (comma separated)
            </label>
            <Input 
              value={sectors} 
              onChange={(e) => setSectors(e.target.value)} 
              placeholder="Employment, Taxes" 
              className="h-9" 
            />
          </div>
          <div>
            <label className="mb-1.5 block text-[11px] font-bold uppercase text-muted-foreground">
              Min Year
            </label>
            <Input 
              type="number"
              value={minYear} 
              onChange={(e) => setMinYear(e.target.value)} 
              className="h-9" 
            />
          </div>
          <div>
            <label className="mb-1.5 block text-[11px] font-bold uppercase text-muted-foreground">
              Legal Types (comma separated)
            </label>
            <Input 
              value={legalTypes} 
              onChange={(e) => setLegalTypes(e.target.value)} 
              placeholder="Nghị định, Thông tư" 
              className="h-9" 
            />
          </div>
          <div>
            <label className="mb-1.5 block text-[11px] font-bold uppercase text-muted-foreground">
              Max Docs (empty for all)
            </label>
            <Input 
              type="number"
              value={maxDocs} 
              onChange={(e) => setMaxDocs(e.target.value)} 
              placeholder="e.g. 100 for testing" 
              className="h-9" 
            />
          </div>
          <div className="pt-4 flex gap-2">
            <Button onClick={handleIngest} className="flex-1">Start Ingestion</Button>
            <Button variant="outline" onClick={() => setIngestModalOpen(false)}>Cancel</Button>
          </div>
        </div>
      </DetailModal>
    </div>
  );
}

function DetailHeader({ rows }: { rows: [string, string][] }) {
  return (
    <div className="rounded-xl border border-border bg-secondary/60 p-3 text-xs">
      {rows.map(([k, v]) => (
        <div key={k} className="flex flex-wrap gap-1 py-0.5">
          <span className="font-bold text-ocean-deep">{k}:</span>
          <span className="font-mono text-[11px] text-foreground">{v}</span>
        </div>
      ))}
    </div>
  );
}

function PreBlock({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-border bg-card p-3">
      <div className="mb-1.5 text-xs font-bold text-ocean-deep">{title}</div>
      <pre className="max-h-72 overflow-auto whitespace-pre-wrap font-mono text-[11px] leading-relaxed text-foreground scrollbar-thin">
        {children}
      </pre>
    </div>
  );
}
