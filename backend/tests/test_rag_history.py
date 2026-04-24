import os
import sys
from types import SimpleNamespace
from types import ModuleType

from rag.prompt_builder import build_prompt


class _FakeCollection:
    def count(self):
        return 0

    def get(self, *args, **kwargs):
        return {"ids": [], "documents": [], "metadatas": []}

    def query(self, *args, **kwargs):
        return {"ids": [[]], "documents": [[]], "distances": [[]]}

    def get_or_create_collection(self, *args, **kwargs):
        return self


if "chromadb" not in sys.modules:
    fake_chromadb = ModuleType("chromadb")
    fake_chromadb.PersistentClient = lambda *args, **kwargs: _FakeCollection()
    sys.modules["chromadb"] = fake_chromadb

os.environ.setdefault("OPENAI_API_KEY", "test-key")

from rag import core


def test_build_prompt_includes_history_before_latest_user_message():
    history = [
        {"role": "user", "content": "Who is the CEO?"},
        {"role": "assistant", "content": "Mr. Nguyen Van A."},
    ]

    messages = build_prompt(
        context="HR policy context",
        question="What is his email?",
        history=history,
    )

    assert messages[0]["role"] == "system"
    assert messages[1] == history[0]
    assert messages[2] == history[1]
    assert messages[3]["role"] == "user"
    assert "USER INPUT" in messages[3]["content"]
    assert "What is his email?" in messages[3]["content"]


def test_query_loads_history_and_passes_it_to_rewrite_and_prompt(monkeypatch):
    history = [
        {"role": "user", "content": "Who is the CEO?"},
        {"role": "assistant", "content": "Mr. Nguyen Van A."},
    ]
    captured: dict[str, object] = {}

    monkeypatch.setattr(core, "get_recent_messages", lambda session_id: history)
    monkeypatch.setattr(core, "detect_language", lambda question: "en")
    monkeypatch.setattr(core.collection, "count", lambda: 1)

    def fake_rewrite_query(question, history=None):
        captured["rewrite_question"] = question
        captured["rewrite_history"] = history
        return "email cua Nguyen Van A"

    def fake_build_prompt(context, question, history=None):
        captured["prompt_context"] = context
        captured["prompt_question"] = question
        captured["prompt_history"] = history
        return [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]

    monkeypatch.setattr(core, "rewrite_query", fake_rewrite_query)
    monkeypatch.setattr(core, "build_prompt", fake_build_prompt)
    monkeypatch.setattr(core, "vector_search", lambda *args, **kwargs: ([], [], []))
    monkeypatch.setattr(core._idx, "search", lambda *args, **kwargs: [])

    fake_response = SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(
                    content=(
                        "Tài liệu hiện tại không có thông tin về vấn đề này.\n"
                        "NGUỒN: Không có thông tin\n"
                        "ĐỘ TIN CẬY: THẤP"
                    )
                )
            )
        ]
    )
    monkeypatch.setattr(
        core.client.chat.completions,
        "create",
        lambda **kwargs: fake_response,
    )

    result = core.query("What is his email?", session_id="session-1")

    assert captured["rewrite_question"] == "What is his email?"
    assert captured["rewrite_history"] == history
    assert captured["prompt_history"] == history
    assert captured["prompt_context"] == core.NO_RESULTS_SENTINEL
    assert result["rewritten_query"] == "email cua Nguyen Van A"
    assert result["confidence"] == "THẤP"
