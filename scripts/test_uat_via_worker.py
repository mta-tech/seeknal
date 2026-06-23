"""Run all 5 UAT CLARIF prompts via worker (gemini-3-flash).

POST /ask ke gateway localhost:8765, capture answer, detect clarification.
"""
from __future__ import annotations

import json
import sys
import time
import urllib.request

GATEWAY = "http://127.0.0.1:8765"

CLARIF_PROMPTS = [
    {
        "id": "CLARIF-BAYI-1",
        "prompt": "berapa total produk formula bayi terdaftar di BPOM?",
        "ambiguity": "ERBA (1301/1302) vs ERLA (622/604/624) vs gabungan",
    },
    {
        "id": "CLARIF-DISERAHKAN-1",
        "prompt": "berapa permohonan?",
        "ambiguity": "'masuk' = jenis_permohonan 301/302/303/304/305 atau subset",
    },
    {
        "id": "CLARIF-KEMASAN-1",
        "prompt": "berapa produk dengan kemasan tunggal?",
        "ambiguity": "ERBA kemasan_id=2 vs ERLA kemasan_id berbeda",
    },
    {
        "id": "CLARIF-KOMITMEN-1",
        "prompt": "berapa produk MR yang sudah selesai proses komitmennya?",
        "ambiguity": "status 4 / 7 / 4+7 / 5 (dibatalkan)",
    },
    {
        "id": "CLARIF-LC-AKTIF-1",
        "prompt": "berapa izin edar yang aktif?",
        "ambiguity": "status 0999 / 0999+0906+9999 / semua yang belum expire",
    },
]


def looks_like_clarification(answer: str) -> bool:
    if not answer or not answer.strip():
        return False
    has_q = "?" in answer
    is_short = len(answer) < 600
    import re

    has_big_number = bool(re.search(r"\b\d{3,}\b", answer))
    return has_q and is_short and not has_big_number


def run_one(item):
    sid = f"flash-{item['id']}"
    body = json.dumps({"question": item["prompt"], "session_id": sid}).encode()
    req = urllib.request.Request(
        f"{GATEWAY}/ask",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            raw = resp.read().decode()
    except Exception as exc:
        return {"id": item["id"], "error": f"{type(exc).__name__}: {exc}", "elapsed": 0}
    elapsed = time.monotonic() - start

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"id": item["id"], "error": "non-JSON response", "elapsed": elapsed}

    answer = data.get("answer", "")
    events = data.get("events", [])
    tool_count = sum(1 for e in events if e.get("type") == "tool_start")
    sql_count = sum(
        1 for e in events if e.get("type") == "tool_start" and e.get("data", {}).get("name") == "execute_sql"
    )
    is_clarif = looks_like_clarification(answer)

    return {
        "id": item["id"],
        "prompt": item["prompt"],
        "ambiguity": item["ambiguity"],
        "answer": answer,
        "answer_len": len(answer),
        "elapsed_s": round(elapsed, 1),
        "tool_calls": tool_count,
        "sql_calls": sql_count,
        "is_clarification": is_clarif,
    }


def main():
    print("=" * 78)
    print("WORKER CLARIFICATION TEST — gemini-3-flash via gateway")
    print("=" * 78)
    results = []
    for item in CLARIF_PROMPTS:
        print(f"\n>>> [{item['id']}] {item['prompt']}")
        print(f"    ambiguity: {item['ambiguity']}")
        r = run_one(item)
        results.append(r)
        if r.get("error"):
            print(f"    ERROR: {r['error']}")
            continue
        verdict = "CLARIFY" if r["is_clarification"] else "DIRECT"
        print(f"    [{verdict}] tools={r['tool_calls']} sql={r['sql_calls']} "
              f"len={r['answer_len']} {r['elapsed_s']}s")
        print(f"    answer: {r['answer'][:280]}")

    print("\n" + "#" * 78)
    print("SUMMARY")
    print("#" * 78)
    clarif_count = sum(1 for r in results if r.get("is_clarification"))
    total = len([r for r in results if not r.get("error")])
    for r in results:
        if r.get("error"):
            print(f"  [ERR ] {r['id']}: {r['error']}")
            continue
        v = "CLARIFY" if r["is_clarification"] else "DIRECT "
        print(f"  [{v}] {r['id']}  tools={r['tool_calls']}/sql={r['sql_calls']}  "
              f"len={r['answer_len']}/{r['elapsed_s']}s")
    print(f"\n  Clarification triggered: {clarif_count}/{total}")
    rate = (clarif_count / total * 100) if total else 0
    print(f"  Clarification rate     : {rate:.0f}%")

    out = {
        "model": "gemini-3-flash",
        "channel": "worker (gateway /ask → environment=gateway)",
        "summary": {
            "total": total,
            "clarified": clarif_count,
            "rate_pct": round(rate, 1),
        },
        "results": results,
    }
    print(f"\n  JSON: {json.dumps(out, indent=2)[:500]}...")
    return out


if __name__ == "__main__":
    main()
