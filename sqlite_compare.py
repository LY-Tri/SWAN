#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
import os
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable


def _stable_key(x: Any) -> str:
    # Stable serialization so we can multiset-intersect rows/items exactly.
    return json.dumps(x, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _as_counter(answer: Any) -> Counter[str]:
    # Answer format is expected to be list-of-rows (list of lists), but we defensively
    # handle non-lists by treating them as a single item.
    if isinstance(answer, list):
        return Counter(_stable_key(item) for item in answer)
    return Counter({_stable_key(answer): 1})


def _intersection_count(a: Counter[str], b: Counter[str]) -> int:
    return sum((a & b).values())


@dataclass(frozen=True)
class FileStats:
    file: str
    n: int
    exact: int
    missing_in_new: int
    missing_in_gold: int
    gold_items: int
    matched_items: int


def _load_jsonl_by_qid(path: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            qid = rec.get("question_id")
            if not isinstance(qid, str) or not qid:
                raise ValueError(f"{path}:{line_no} missing/invalid question_id")
            out[qid] = rec
    return out


def _iter_shared_files(gold_dir: str, new_dir: str, pattern: str) -> Iterable[tuple[str, str, str]]:
    gold_files = {os.path.basename(p): p for p in glob.glob(os.path.join(gold_dir, pattern))}
    new_files = {os.path.basename(p): p for p in glob.glob(os.path.join(new_dir, pattern))}
    for name in sorted(set(gold_files) | set(new_files)):
        yield name, gold_files.get(name, ""), new_files.get(name, "")


def compare_file(name: str, gold_path: str, new_path: str) -> FileStats:
    gold = _load_jsonl_by_qid(gold_path) if gold_path else {}
    new = _load_jsonl_by_qid(new_path) if new_path else {}

    all_qids = sorted(set(gold) | set(new))
    exact = 0
    missing_in_new = 0
    missing_in_gold = 0
    gold_items = 0
    matched_items = 0

    for qid in all_qids:
        if qid not in new:
            missing_in_new += 1
            continue
        if qid not in gold:
            missing_in_gold += 1
            continue

        g_ans = gold[qid].get("answer")
        n_ans = new[qid].get("answer")

        if g_ans == n_ans:
            exact += 1

        g_ctr = _as_counter(g_ans)
        n_ctr = _as_counter(n_ans)
        gold_items += sum(g_ctr.values())
        matched_items += _intersection_count(g_ctr, n_ctr)

    n = len(all_qids) - missing_in_new - missing_in_gold
    return FileStats(
        file=name,
        n=n,
        exact=exact,
        missing_in_new=missing_in_new,
        missing_in_gold=missing_in_gold,
        gold_items=gold_items,
        matched_items=matched_items,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare SWAN gold execution answers vs new execution answers.\n\n"
            "Exact accuracy: fraction of question_ids with identical 'answer'.\n"
            "Fine-grained accuracy (list overlap): total matched list-items / total gold list-items."
        )
    )
    parser.add_argument(
        "--gold-dir",
        default=os.path.join(os.path.dirname(__file__), "gold_answers"),
        help="Directory containing baseline *_gold.jsonl files (default: SWAN/gold_answers).",
    )
    parser.add_argument(
        "--new-dir",
        default=os.path.join(os.path.dirname(__file__), "gold_answers", "new"),
        help="Directory containing new *_gold.jsonl files (default: SWAN/gold_answers/new).",
    )
    parser.add_argument(
        "--pattern",
        default="*_gold.jsonl",
        help='Filename glob to compare within each dir (default: "*_gold.jsonl").',
    )
    args = parser.parse_args()

    gold_dir = os.path.abspath(args.gold_dir)
    new_dir = os.path.abspath(args.new_dir)

    total_n = 0
    total_exact = 0
    total_missing_in_new = 0
    total_missing_in_gold = 0
    total_gold_items = 0
    total_matched_items = 0

    any_files = False
    for name, gold_path, new_path in _iter_shared_files(gold_dir, new_dir, args.pattern):
        if not gold_path and not new_path:
            continue
        any_files = True
        if not gold_path:
            stats = FileStats(
                file=name,
                n=0,
                exact=0,
                missing_in_new=0,
                missing_in_gold=len(_load_jsonl_by_qid(new_path)),
                gold_items=0,
                matched_items=0,
            )
        elif not new_path:
            stats = FileStats(
                file=name,
                n=0,
                exact=0,
                missing_in_new=len(_load_jsonl_by_qid(gold_path)),
                missing_in_gold=0,
                gold_items=0,
                matched_items=0,
            )
        else:
            stats = compare_file(name, gold_path, new_path)

        total_n += stats.n
        total_exact += stats.exact
        total_missing_in_new += stats.missing_in_new
        total_missing_in_gold += stats.missing_in_gold
        total_gold_items += stats.gold_items
        total_matched_items += stats.matched_items

        exact_acc = (stats.exact / stats.n) if stats.n else 0.0
        fine_acc = (stats.matched_items / stats.gold_items) if stats.gold_items else 0.0
        print(
            f"{stats.file}: n={stats.n} exact={stats.exact} ({exact_acc:.3f}) "
            f"fine={stats.matched_items}/{stats.gold_items} ({fine_acc:.3f}) "
            f"missing_in_new={stats.missing_in_new} missing_in_gold={stats.missing_in_gold}"
        )

    if not any_files:
        raise SystemExit(
            f"No files matched. Looked for {os.path.join(gold_dir, args.pattern)} "
            f"and {os.path.join(new_dir, args.pattern)}"
        )

    overall_exact = (total_exact / total_n) if total_n else 0.0
    overall_fine = (total_matched_items / total_gold_items) if total_gold_items else 0.0
    print(
        f"OVERALL: n={total_n} exact={total_exact} ({overall_exact:.3f}) "
        f"fine={total_matched_items}/{total_gold_items} ({overall_fine:.3f}) "
        f"missing_in_new={total_missing_in_new} missing_in_gold={total_missing_in_gold}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

