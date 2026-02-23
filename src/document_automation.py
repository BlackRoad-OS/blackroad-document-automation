#!/usr/bin/env python3
"""BlackRoad Document Automation — template rendering, variable substitution,
and export tracking."""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
CYAN   = "\033[0;36m"
BOLD   = "\033[1m"
NC     = "\033[0m"

DB_PATH  = Path.home() / ".blackroad" / "document_automation.db"
DOCS_DIR = Path.home() / ".blackroad" / "documents"


# ── Data models ───────────────────────────────────────────────────────────────
@dataclass
class Template:
    id: Optional[int]
    name: str
    content: str            # body with {{variable}} placeholders
    variables: str          # JSON list of required variable names
    category: str
    version: int = 1
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class Document:
    id: Optional[int]
    template_id: int
    template_name: str
    title: str
    content: str
    variables_used: str     # JSON-encoded variable dict
    fmt: str                # txt | html | md
    status: str             # draft | final | exported
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class ExportRecord:
    id: Optional[int]
    document_id: int
    export_path: str
    export_format: str
    file_size_bytes: int
    exported_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ── Database ──────────────────────────────────────────────────────────────────
def _get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        "CREATE TABLE IF NOT EXISTS templates ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  name TEXT NOT NULL UNIQUE,"
        "  content TEXT NOT NULL,"
        "  variables TEXT DEFAULT '[]',"
        "  category TEXT DEFAULT 'general',"
        "  version INTEGER DEFAULT 1,"
        "  created_at TEXT NOT NULL,"
        "  updated_at TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS documents ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  template_id INTEGER,"
        "  template_name TEXT NOT NULL,"
        "  title TEXT NOT NULL,"
        "  content TEXT NOT NULL,"
        "  variables_used TEXT DEFAULT '{}',"
        "  fmt TEXT DEFAULT 'txt',"
        "  status TEXT DEFAULT 'draft',"
        "  created_at TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS export_records ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  document_id INTEGER NOT NULL,"
        "  export_path TEXT NOT NULL,"
        "  export_format TEXT NOT NULL,"
        "  file_size_bytes INTEGER DEFAULT 0,"
        "  exported_at TEXT NOT NULL"
        ");"
    )
    conn.commit()
    return conn


# ── Rendering ─────────────────────────────────────────────────────────────────
def _render(content: str, variables: Dict[str, str]) -> str:
    """Substitute {{variable}} placeholders. Raises KeyError for missing vars."""
    def replace(m: re.Match) -> str:
        key = m.group(1).strip()
        if key not in variables:
            raise KeyError(f"Missing template variable: '{key}'")
        return str(variables[key])
    return re.sub(r"\{\{([^}]+)\}\}", replace, content)


def _extract_vars(content: str) -> List[str]:
    """Return de-duplicated list of placeholder names found in content."""
    return list(dict.fromkeys(
        m.strip() for m in re.findall(r"\{\{([^}]+)\}\}", content)
    ))


# ── Engine ────────────────────────────────────────────────────────────────────
class DocumentEngine:
    """Template management, document rendering, and export tracking."""

    def __init__(self) -> None:
        self._db = _get_db()

    # ── Template CRUD ──────────────────────────────────────────────────────
    def create_template(self, name: str, content: str,
                        category: str = "general") -> Template:
        """Upsert a template; bumps version on update."""
        variables = json.dumps(_extract_vars(content))
        ts = datetime.utcnow().isoformat()
        existing = self._db.execute(
            "SELECT id, version FROM templates WHERE name=?", (name,)
        ).fetchone()
        if existing:
            ver = existing["version"] + 1
            self._db.execute(
                "UPDATE templates"
                " SET content=?, variables=?, category=?,"
                "     version=?, updated_at=? WHERE name=?",
                (content, variables, category, ver, ts, name),
            )
            self._db.commit()
            tid = existing["id"]
        else:
            ver = 1
            cur = self._db.execute(
                "INSERT INTO templates"
                " (name, content, variables, category, version,"
                "  created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (name, content, variables, category, ver, ts, ts),
            )
            self._db.commit()
            tid = cur.lastrowid
        return Template(id=tid, name=name, content=content,
                        variables=variables, category=category,
                        version=ver, created_at=ts, updated_at=ts)

    # ── Rendering ──────────────────────────────────────────────────────────
    def render(self, template_name: str, title: str,
               variables: Dict[str, str], fmt: str = "txt") -> Document:
        """Render a named template with variable substitution."""
        row = self._db.execute(
            "SELECT * FROM templates WHERE name=?", (template_name,)
        ).fetchone()
        if not row:
            raise KeyError(f"Template '{template_name}' not found")
        try:
            content = _render(row["content"], variables)
        except KeyError as exc:
            raise ValueError(str(exc)) from exc
        ts = datetime.utcnow().isoformat()
        cur = self._db.execute(
            "INSERT INTO documents"
            " (template_id, template_name, title, content,"
            "  variables_used, fmt, status, created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (row["id"], template_name, title, content,
             json.dumps(variables), fmt, "draft", ts),
        )
        self._db.commit()
        return Document(
            id=cur.lastrowid, template_id=row["id"],
            template_name=template_name, title=title,
            content=content, variables_used=json.dumps(variables),
            fmt=fmt, status="draft", created_at=ts,
        )

    # ── Export ─────────────────────────────────────────────────────────────
    def export_document(self, doc_id: int,
                        fmt: Optional[str] = None) -> ExportRecord:
        """Write a rendered document to disk and record the export."""
        row = self._db.execute(
            "SELECT * FROM documents WHERE id=?", (doc_id,)
        ).fetchone()
        if not row:
            raise KeyError(f"Document id={doc_id} not found")
        export_fmt = fmt or row["fmt"]
        safe_title = re.sub(r"[^\w\-]", "_", row["title"])
        ts_tag = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fname = f"{safe_title}_{ts_tag}.{export_fmt}"
        out_path = DOCS_DIR / fname

        body = row["content"]
        if export_fmt == "html":
            body = (
                f"<!DOCTYPE html><html><head>"
                f"<title>{row['title']}</title></head>"
                f"<body><h1>{row['title']}</h1>"
                f"<pre>{body}</pre></body></html>"
            )
        elif export_fmt == "md":
            body = f"# {row['title']}\n\n{body}\n"

        out_path.write_text(body, encoding="utf-8")
        size = out_path.stat().st_size
        ts = datetime.utcnow().isoformat()
        cur = self._db.execute(
            "INSERT INTO export_records"
            " (document_id, export_path, export_format,"
            "  file_size_bytes, exported_at)"
            " VALUES (?,?,?,?,?)",
            (doc_id, str(out_path), export_fmt, size, ts),
        )
        self._db.execute(
            "UPDATE documents SET status='exported' WHERE id=?", (doc_id,))
        self._db.commit()
        return ExportRecord(
            id=cur.lastrowid, document_id=doc_id,
            export_path=str(out_path), export_format=export_fmt,
            file_size_bytes=size, exported_at=ts,
        )

    # ── Listing & stats ────────────────────────────────────────────────────
    def list_documents(self, limit: int = 25) -> List[dict]:
        rows = self._db.execute(
            "SELECT id, template_name, title, fmt, status, created_at"
            " FROM documents ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def list_templates(self) -> List[dict]:
        rows = self._db.execute(
            "SELECT name, category, version, variables, updated_at"
            " FROM templates ORDER BY name"
        ).fetchall()
        return [dict(r) for r in rows]

    def pipeline_status(self) -> dict:
        db = self._db
        return {
            "templates": db.execute(
                "SELECT COUNT(*) FROM templates").fetchone()[0],
            "documents": db.execute(
                "SELECT COUNT(*) FROM documents").fetchone()[0],
            "drafts":    db.execute(
                "SELECT COUNT(*) FROM documents"
                " WHERE status='draft'").fetchone()[0],
            "exported":  db.execute(
                "SELECT COUNT(*) FROM documents"
                " WHERE status='exported'").fetchone()[0],
            "total_exports": db.execute(
                "SELECT COUNT(*) FROM export_records").fetchone()[0],
        }


# ── Helpers ───────────────────────────────────────────────────────────────────
def _status_colour(s: str) -> str:
    return {
        "draft":    f"{YELLOW}{s}{NC}",
        "final":    f"{GREEN}{s}{NC}",
        "exported": f"{CYAN}{s}{NC}",
    }.get(s, s)


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        prog="document_automation",
        description=(f"{BOLD}BlackRoad Document Automation{NC}"
                     " — templates, rendering, export"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="cmd", metavar="command")

    p_list = sub.add_parser("list", help="List documents or templates")
    p_list.add_argument(
        "--type", choices=["docs", "templates"], default="docs")
    p_list.add_argument("--limit", type=int, default=25)

    p_add = sub.add_parser("add", help="Create or update a template")
    p_add.add_argument("name")
    p_add.add_argument("--content",  required=True,
                       help="Template body with {{variable}} placeholders")
    p_add.add_argument("--category", default="general")

    p_render = sub.add_parser("render", help="Render a template to a document")
    p_render.add_argument("template")
    p_render.add_argument("title")
    p_render.add_argument("--vars",   default="{}",
                          help='JSON variable dict e.g. {"name":"Alice"}')
    p_render.add_argument("--format", default="txt",
                          choices=["txt", "html", "md"])

    sub.add_parser("status", help="Show pipeline statistics")

    p_export = sub.add_parser("export", help="Export a document to file")
    p_export.add_argument("doc_id", type=int)
    p_export.add_argument("--format", default=None,
                          choices=["txt", "html", "md"])

    args = parser.parse_args()
    engine = DocumentEngine()

    if args.cmd == "list":
        if args.type == "templates":
            rows = engine.list_templates()
            print(f"\n{BOLD}{CYAN}\U0001f4c4 Templates ({len(rows)}){NC}\n")
            for t in rows:
                vars_list = json.loads(t["variables"])
                ts = t["updated_at"][:10]
                print(f"  {BOLD}{t['name']:<28}{NC}"
                      f" v{t['version']}  [{t['category']}]"
                      f"  vars={vars_list}  updated={ts}")
        else:
            rows = engine.list_documents(args.limit)
            print(f"\n{BOLD}{CYAN}\U0001f4dd Documents ({len(rows)}){NC}\n")
            for d in rows:
                ts = d["created_at"][:19].replace("T", " ")
                print(f"  {d['id']:>4}  {d['title']:<32}"
                      f"  {_status_colour(d['status']):<20}  {ts}")
        print()

    elif args.cmd == "add":
        t = engine.create_template(
            args.name, args.content, category=args.category)
        vars_list = json.loads(t.variables)
        print(f"{GREEN}\u2705 Template '{t.name}'{NC}"
              f"  v{t.version}  vars={vars_list}")

    elif args.cmd == "render":
        try:
            variables = json.loads(args.vars)
        except json.JSONDecodeError:
            print(f"{RED}\u2717 --vars must be valid JSON{NC}", file=sys.stderr)
            sys.exit(1)
        try:
            doc = engine.render(
                args.template, args.title, variables, fmt=args.format)
        except (KeyError, ValueError) as exc:
            print(f"{RED}\u2717 {exc}{NC}", file=sys.stderr)
            sys.exit(1)
        print(f"{GREEN}\u2705 Document rendered{NC}"
              f"  id={doc.id}  [{doc.fmt}]  '{doc.title}'")
        print(f"\n{CYAN}\u2500\u2500\u2500 Preview \u2500\u2500\u2500{NC}")
        preview = doc.content[:400]
        print(preview + ("\u2026" if len(doc.content) > 400 else ""))

    elif args.cmd == "status":
        s = engine.pipeline_status()
        print(f"\n{BOLD}{CYAN}\U0001f4ca Document Automation — Status{NC}\n")
        print(f"  Templates : {BOLD}{s['templates']}{NC}")
        print(f"  Documents : {BOLD}{s['documents']}{NC}"
              f"  ({YELLOW}draft: {s['drafts']}{NC}"
              f"  {CYAN}exported: {s['exported']}{NC})")
        print(f"  Exports   : {s['total_exports']}\n")

    elif args.cmd == "export":
        try:
            rec = engine.export_document(args.doc_id, fmt=args.format)
        except KeyError as exc:
            print(f"{RED}\u2717 {exc}{NC}", file=sys.stderr)
            sys.exit(1)
        print(f"{GREEN}\u2705 Exported \u2192{NC} {rec.export_path}"
              f"  ({rec.file_size_bytes} bytes)")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
