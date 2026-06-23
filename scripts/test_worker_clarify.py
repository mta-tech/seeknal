"""Pure worker clarification-gate test.

Menguji apakah clarification question aktif di environment="gateway" (worker mode).

Cara pakai:
    # 1. Pakai project seeknal-bpom-neo (BPOM) sebagai target
    export TARGET_PROJECT=/home/mta/projects/seeknal_audit/seeknal-bpom-neo
    export WAREHOUSE_URL=postgresql://readonly_user:read_only_seeknal@localhost:5533/rpo_v2
    export GOOGLE_API_KEY=...

    # 2. Jalankan test (dari root seeknal repo)
    PYTHONPATH=src .venv/bin/python scripts/test_worker_clarify.py

Logika:
    - Buat agent dengan environment="gateway" (simulasi worker)
    - Kirim pertanyaan ambigu dari CLARIF scenarios
    - Verifikasi: turn pertama HARUS mengandung pertanyaan klarifikasi
      (bukan langsung memberi angka/jawaban final)
    - Verifikasi: setelah user jawab klarifikasi, agent mengeksekusi SQL

Skenario diambil dari seeknal-bpom-neo/seeknal/tests/v1/multiturn/UAT/CLARIF-*.yml
dan di-embed langsung di sini supaya script self-contained (tidak depend
path luar).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))


@dataclass
class ClarifTurn:
    prompt: str
    expect_clarification: bool
    assert_contains: list[str] = field(default_factory=list)
    note: str = ""


@dataclass
class ClarifScenario:
    scenario_id: str
    name: str
    description: str
    turns: list[ClarifTurn]


SCENARIOS: list[ClarifScenario] = [
    ClarifScenario(
        scenario_id="CLARIF-BAYI-1",
        name="Clarification_Gate_Formula_Bayi",
        description=(
            "Material ambiguity SOURCE_PATH: 'terdaftar di BPOM' tidak spesifik "
            "sistem. ERBA pakai kode 1301/1302, ERLA pakai 622/604/624. "
            "Agent HARUS tanya scope sistem sebelum query."
        ),
        turns=[
            ClarifTurn(
                prompt="berapa total produk formula bayi terdaftar di BPOM?",
                expect_clarification=True,
                assert_contains=["formula bayi"],
                note="Turn 1: ambigu ERBA/ERLA/gabungan. Worker harus tanya klarifikasi.",
            ),
            ClarifTurn(
                prompt="gabungkan keduanya",
                expect_clarification=False,
                assert_contains=["916"],
                note="Turn 2: setelah klarifikasi. ERBA 102 + ERLA 814 = 916.",
            ),
        ],
    ),
    ClarifScenario(
        scenario_id="CLARIF-KOMITMEN-1",
        name="Clarification_Gate_Komitmen_Disetujui",
        description=(
            "Material ambiguity EXACT_VS_FAMILY: 'selesai proses komitmen' bisa "
            "berarti status 4, 7, 4+7, atau 5. Agent harus klarifikasi."
        ),
        turns=[
            ClarifTurn(
                prompt="berapa produk MR yang sudah selesai proses komitmennya?",
                expect_clarification=True,
                assert_contains=["komitmen"],
                note="Turn 1: ambigu status. Worker harus tanya klarifikasi.",
            ),
            ClarifTurn(
                prompt="yang disetujui saja, tanpa catatan",
                expect_clarification=False,
                assert_contains=["2.717", "Menengah Rendah"],
                note="Turn 2: lock status_komitmen=4, hasil 2.717.",
            ),
        ],
    ),
    ClarifScenario(
        scenario_id="CLARIF-NEUTRAL-1",
        name="Non_Ambiguous_Control",
        description=(
            "Control test: pertanyaan spesifik (tidak ambigu). Worker TIDAK boleh "
            "klarifikasi, langsung jawab. Mencegah false positive."
        ),
        turns=[
            ClarifTurn(
                prompt="apa itu NIE?",
                expect_clarification=False,
                assert_contains=["Nomor Izin Edar"],
                note="Turn 1: konseptual spesifik. Tidak boleh klarifikasi.",
            ),
        ],
    ),
]


CLARIF_HINTS = [
    "atau", "mana yang", "mana yang kamu", "pilih", "scope", "sistem",
    "spesifik", "tahun", "rentang", "jelaskan", "kriteria",
    "status", "kategori", "mana", "apakah", "apakah anda",
]


def looks_like_clarification(answer: str) -> bool:
    """Heuristic ketat: apakah response adalah pertanyaan klarifikasi?

    Sebuah response dianggap clarification bila SEMUA kondisi terpenuhi:
    1. Mengandung tanda tanya '?' (mandatory - clarification is a question)
    2. Relatif pendek (<500 char) - panjang = analisis, bukan klarifikasi
    3. Mengandung minimal 1 kata kunci klarifikasi (atau/mana/pilih/sistem/...)
    4. TIDAK mengandung angka hasil besar (data spesifik = sudah query DB)
    """
    if not answer or not answer.strip():
        return False
    lower = answer.lower()
    has_qmark = "?" in answer
    is_short = len(answer) < 500
    hint_hits = sum(1 for h in CLARIF_HINTS if h in lower)
    has_hint = hint_hits >= 1
    import re as _re

    has_big_number = bool(_re.search(r"\b\d{2,}\b", answer))
    return has_qmark and is_short and has_hint and not has_big_number


def _extract_sqls(history) -> list[str]:
    from pydantic_ai.messages import ToolCallPart

    sqls = []
    for m in history:
        for p in getattr(m, "parts", []):
            if isinstance(p, ToolCallPart) and p.tool_name == "execute_sql":
                args = p.args if isinstance(p.args, dict) else {}
                sql = (args.get("sql") or args.get("query") or "").strip()
                if sql:
                    sqls.append(sql)
    return sqls


def _load_env(project_path: Path) -> None:
    from seeknal.cli.ask import _load_project_env

    _load_project_env(project_path)


def run_scenario(
    scenario: ClarifScenario,
    project_path: Path,
    turn_timeout: int = 0,
) -> list[dict]:
    from seeknal.ask.agents.agent import ask as agent_ask, create_agent
    from seeknal.ask.agents.tools._context import reset_turn_governor
    from pydantic_ai.messages import ModelResponse, ToolCallPart

    print("\n" + "=" * 70)
    print(f"SCENARIO [{scenario.scenario_id}] {scenario.name}")
    print(f"  {scenario.description}")
    print("=" * 70)

    agent, deps, history, _ = create_agent(project_path, environment="gateway")
    print(f"[init] Agent created environment=gateway — shared history")

    results: list[dict] = []

    for i, turn in enumerate(scenario.turns, start=1):
        print("\n" + "-" * 60)
        print(f"TURN {i}/{len(scenario.turns)}")
        print(f"  prompt : {turn.prompt}")
        print(f"  expect : {'CLARIFY' if turn.expect_clarification else 'ANSWER'}")
        print(f"  note   : {turn.note}")

        prev_len = len(history)
        reset_turn_governor(turn.prompt)
        start = time.monotonic()
        answer = ""
        error = None

        if turn_timeout > 0:
            import signal

            def _handler(signum, frame):
                raise TimeoutError(f"turn > {turn_timeout}s")

            old = signal.signal(signal.SIGALRM, _handler)
            signal.alarm(turn_timeout)
            try:
                answer = agent_ask(agent, deps, history, turn.prompt)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                answer = f"[ERROR] {error}"
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old)
        else:
            try:
                answer = agent_ask(agent, deps, history, turn.prompt)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                answer = f"[ERROR] {error}"

        elapsed = time.monotonic() - start
        new_msgs = history[prev_len:]
        llm_req = sum(1 for m in new_msgs if isinstance(m, ModelResponse))
        tool_calls = sum(
            1
            for m in new_msgs
            for p in getattr(m, "parts", [])
            if isinstance(p, ToolCallPart)
        )
        sqls = _extract_sqls(new_msgs)

        actually_clarifies = looks_like_clarification(answer)
        expectation_met = actually_clarifies == turn.expect_clarification

        content_failures = []
        if not error:
            for expected in turn.assert_contains:
                if expected.lower() not in answer.lower():
                    content_failures.append(f"missing: '{expected}'")

        gate_pass = expectation_met
        content_pass = not content_failures
        overall_pass = gate_pass and content_pass and error is None

        results.append(
            {
                "turn_num": i,
                "prompt": turn.prompt,
                "answer": answer,
                "elapsed_s": round(elapsed, 2),
                "llm_requests": llm_req,
                "tool_calls": tool_calls,
                "sqls": sqls,
                "expected_clarification": turn.expect_clarification,
                "actually_clarifies": actually_clarifies,
                "gate_pass": gate_pass,
                "content_pass": content_pass,
                "content_failures": content_failures,
                "overall_pass": overall_pass,
                "error": error,
            }
        )

        status = "PASS" if overall_pass else "FAIL"
        gate_label = "CLARIFY" if actually_clarifies else "ANSWER"
        print(f"  result : [{status}] gate={gate_label}/{turn.expect_clarification} "
              f"content={'OK' if content_pass else 'FAIL'}")
        print(f"  metric : {elapsed:.1f}s llm={llm_req} tools={tool_calls} sql={len(sqls)}")
        if content_failures:
            for f in content_failures:
                print(f"    CONTENT FAIL: {f}")
        if error:
            print(f"    ERROR: {error}")
        print(f"  answer : {answer[:300]}")

    return results


def print_summary(all_results: list[tuple[ClarifScenario, list[dict]]]) -> dict:
    print("\n" + "#" * 70)
    print("# WORKER CLARIFICATION TEST SUMMARY")
    print("#" * 70)

    total_turns = 0
    passed_turns = 0
    gate_total = 0
    gate_passed = 0
    scenarios_passed = 0

    for scenario, results in all_results:
        scenario_pass = all(r["overall_pass"] for r in results)
        if scenario_pass:
            scenarios_passed += 1
        print(
            f"\n  [{'PASS' if scenario_pass else 'FAIL'}] "
            f"[{scenario.scenario_id}] {scenario.name}"
        )
        for r in results:
            total_turns += 1
            gate_total += 1
            status = "PASS" if r["overall_pass"] else "FAIL"
            gate_label = "CLARIFY" if r["actually_clarifies"] else "ANSWER"
            expected_label = "CLARIFY" if r["expected_clarification"] else "ANSWER"
            print(
                f"    [{status}] T{r['turn_num']}: "
                f"gate={gate_label}/{expected_label}  "
                f"{r['elapsed_s']:.1f}s  tools={r['tool_calls']}  sql={len(r['sqls'])}"
            )
            if r["overall_pass"]:
                passed_turns += 1
                gate_passed += 1
            for f in r["content_failures"]:
                print(f"         CONTENT: {f}")

    summary = {
        "total_scenarios": len(all_results),
        "scenarios_passed": scenarios_passed,
        "total_turns": total_turns,
        "turns_passed": passed_turns,
        "gate_total": gate_total,
        "gate_passed": gate_passed,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    print("\n" + "#" * 70)
    print(f"  Scenarios : {scenarios_passed}/{len(all_results)} passed")
    print(f"  Turns     : {passed_turns}/{total_turns} passed")
    print(f"  Gate      : {gate_passed}/{gate_total} expectations met")
    print("#" * 70)
    return summary


def save_output(
    all_results: list[tuple[ClarifScenario, list[dict]]],
    summary: dict,
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"worker_clarify_{ts}.json"

    payload = {
        "mode": "worker-clarify-test",
        "summary": summary,
        "scenarios": [
            {
                "scenario_id": s.scenario_id,
                "name": s.name,
                "description": s.description,
                "turns": results,
            }
            for s, results in all_results
        ],
    }
    out_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test clarification question aktif di worker mode (environment=gateway)"
    )
    parser.add_argument(
        "--project",
        type=str,
        default=os.environ.get(
            "TARGET_PROJECT",
            str(REPO_ROOT.parent / "seeknal-bpom-neo"),
        ),
        help="Path ke project seeknal target (default: seeknal-bpom-neo)",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default=None,
        help="Filter scenario ID (substring match)",
    )
    parser.add_argument(
        "--turn-timeout",
        type=int,
        default=180,
        metavar="SECONDS",
        help="Per-turn SIGALRM timeout (default 180s)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output JSON directory (default: scripts/_outputs/worker_clarify)",
    )
    args = parser.parse_args()

    project_path = Path(args.project).resolve()
    if not project_path.is_dir():
        print(f"ERROR: project path tidak ada: {project_path}", file=sys.stderr)
        return 2

    env_file = project_path / ".env"
    if env_file.exists():
        print(f"[init] Loading env from {env_file}")
    else:
        print(f"[warn] .env tidak ditemukan di {project_path}")

    _load_env(project_path)

    warehouse = os.environ.get("WAREHOUSE_URL")
    llm_provider = os.environ.get("SEEKNAL_ASK_LLM_PROVIDER")
    model = os.environ.get("SEEKNAL_ASK_MODEL")
    print(f"[init] WAREHOUSE_URL={'set' if warehouse else 'MISSING'}")
    print(f"[init] LLM provider={llm_provider} model={model}")
    print(f"[init] project={project_path}")

    scenarios = SCENARIOS
    if args.scenario:
        sel = args.scenario.lower()
        scenarios = [s for s in SCENARIOS if sel in s.scenario_id.lower()]
        if not scenarios:
            print(f"ERROR: tidak ada scenario match '{args.scenario}'", file=sys.stderr)
            return 2

    all_results: list[tuple[ClarifScenario, list[dict]]] = []
    for scenario in scenarios:
        try:
            results = run_scenario(
                scenario, project_path, turn_timeout=args.turn_timeout
            )
        except Exception as exc:
            print(f"\n[FATAL] Scenario {scenario.scenario_id} crash: {exc}")
            results = [
                {
                    "turn_num": 0,
                    "prompt": "",
                    "answer": f"[FATAL] {type(exc).__name__}: {exc}",
                    "elapsed_s": 0.0,
                    "llm_requests": 0,
                    "tool_calls": 0,
                    "sqls": [],
                    "expected_clarification": False,
                    "actually_clarifies": False,
                    "gate_pass": False,
                    "content_pass": False,
                    "content_failures": [f"fatal: {exc}"],
                    "overall_pass": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            ]
        all_results.append((scenario, results))

    summary = print_summary(all_results)

    output_dir = Path(args.output_dir) if args.output_dir else (
        REPO_ROOT / "scripts" / "_outputs" / "worker_clarify"
    )
    out_path = save_output(all_results, summary, output_dir)
    print(f"\n[output] saved: {out_path}")

    return 0 if summary["turns_passed"] == summary["total_turns"] else 1


if __name__ == "__main__":
    sys.exit(main())
