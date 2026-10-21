#!/usr/bin/env python3
"""Streaming MapReduce and independent DuckDB checks for the Python vacancy segment."""
from __future__ import annotations

import argparse
import collections
import csv
import gzip
import hashlib
import io
import itertools
import json
import os
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.client import Config

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "results"
OUT.mkdir(exist_ok=True)
BUCKET = os.getenv("RUSTFS_BUCKET", "hh-lake")
KEY = "vacancies_2024.csv.gz"
ENDPOINT = os.getenv("RUSTFS_ENDPOINT", "http://127.0.0.1:9000")


def s3client():
    return boto3.client("s3", endpoint_url=ENDPOINT,
        aws_access_key_id=os.getenv("RUSTFS_ACCESS_KEY", "de_student"),
        aws_secret_access_key=os.getenv("RUSTFS_SECRET_KEY", "ROTATED_CREDENTIAL_REVOKED"),
        region_name="us-east-1", config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))


def skills_of(raw: str) -> tuple[str, ...]:
    return tuple(sorted({s.strip().casefold() for s in (raw or "").split(";") if s.strip()}))


def selected(row: dict[str, str]) -> bool:
    # Variant = Python. Exact skill token OR title substring; description is deliberately excluded.
    if "python" in (row.get("name") or "").casefold():
        return True
    return any(token.strip().casefold() == "python" for token in (row.get("key_skills") or "").split(";"))


def combos(skills: tuple[str, ...], n: int):
    return (" + ".join(c) for c in itertools.combinations(skills, n))


def emit_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def map_chunk(rows: list[dict[str, str]], fail_at: int | None = None, offset: int = 0):
    counts = [collections.Counter() for _ in range(3)]
    regions = {}
    if fail_at is not None and offset <= fail_at < offset + len(rows):
        raise RuntimeError(f"injected mapper failure at input row {fail_at + 1}")
    selected_rows = 0
    for row in rows:
        if not selected(row):
            continue
        selected_rows += 1
        skills = skills_of(row.get("key_skills", ""))
        for n in (1, 2, 3):
            counts[n - 1].update(combos(skills, n))
        area = (row.get("area") or "").strip() or "(не указан)"
        region = regions.setdefault(area, {"vacancies": 0, "salary_rur": []})
        region["vacancies"] += 1
        if (row.get("currency") or "").upper() == "RUR":
            try:
                low = float(row.get("salary_from") or 0)
                high = float(row.get("salary_to") or 0)
                salary = (low + high) / 2 if low > 0 and high > 0 else low or high
                if salary > 0:
                    region["salary_rur"].append(salary)
            except ValueError:
                pass
    return selected_rows, counts, regions


def mr_run(chunk_rows: int, fail_at: int | None = None, persist: bool = True):
    started = time.perf_counter()
    s3 = s3client()
    body = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"]
    text_stream = io.TextIOWrapper(gzip.GzipFile(fileobj=body), encoding="utf-8", newline="")
    reader = csv.DictReader(text_stream)
    total = selected_total = 0
    reduced = [collections.Counter() for _ in range(3)]
    regions = {}
    while True:
        rows = list(itertools.islice(reader, chunk_rows))
        if not rows:
            break
        mapped, local, local_regions = map_chunk(rows, fail_at=fail_at, offset=total)
        total += len(rows)
        selected_total += mapped
        for i in range(3):
            reduced[i].update(local[i])
        for area, part in local_regions.items():
            target = regions.setdefault(area, {"vacancies": 0, "salary_rur": []})
            target["vacancies"] += part["vacancies"]
            target["salary_rur"].extend(part["salary_rur"])
        if total % (chunk_rows * 10) == 0:
            print(f"read_rows={total} selected={selected_total}", flush=True)
    result = {
        "engine": "custom-streaming-mapreduce", "chunk_rows": chunk_rows,
        "input_rows": total, "python_vacancies": selected_total,
        "counts": {str(i + 1): dict(sorted(reduced[i].items())) for i in range(3)},
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "source": f"s3://{BUCKET}/{KEY}",
        "filter": "case-insensitive 'python' substring in name OR exact semicolon-delimited key_skills token 'python'",
        "top_regions": [{"area": area, "vacancies": item["vacancies"],
            "salary_observations": len(item["salary_rur"]),
            "median_salary_rur": round(statistics.median(item["salary_rur"]), 2) if item["salary_rur"] else None}
            for area, item in sorted(regions.items(), key=lambda kv: kv[1]["vacancies"], reverse=True)[:3]],
    }
    if persist:
        emit_json(OUT / f"mapreduce_{chunk_rows}.json", result)
        s3.put_object(Bucket=BUCKET, Key=f"results/mapreduce_{chunk_rows}.json",
            Body=json.dumps(result, ensure_ascii=False).encode(), ContentType="application/json")
    return result


def duckdb_run():
    import duckdb
    started = time.perf_counter()
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    access = os.getenv("RUSTFS_ACCESS_KEY", "de_student").replace("'", "''")
    secret = os.getenv("RUSTFS_SECRET_KEY", "ROTATED_CREDENTIAL_REVOKED").replace("'", "''")
    endpoint = ENDPOINT.removeprefix("http://").removeprefix("https://").replace("'", "''")
    ssl = "true" if ENDPOINT.startswith("https://") else "false"
    con.execute(f"""CREATE SECRET rustfs_secret (TYPE s3, PROVIDER config,
        KEY_ID '{access}', SECRET '{secret}', REGION 'us-east-1',
        ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL {ssl})""")
    uri = f"s3://{BUCKET}/{KEY}"
    # All input columns remain strings so both engines see precisely the same raw CSV values.
    q = f"""
      WITH raw AS (
        SELECT row_number() OVER () AS rid, name, key_skills
        FROM read_csv('{uri}', header=true, all_varchar=true, nullstr='')
      ), chosen AS (
        SELECT rid, key_skills FROM raw
        WHERE contains(lower(coalesce(name,'')), 'python')
           OR list_contains(list_transform(string_split(coalesce(key_skills,''), ';'), x -> lower(trim(x))), 'python')
      ), skill_rows AS (
        SELECT DISTINCT c.rid, lower(trim(u.skill)) AS skill
        FROM chosen c, unnest(string_split(coalesce(c.key_skills,''), ';')) AS u(skill)
        WHERE trim(u.skill) <> ''
      ), singles AS (
        SELECT 1 AS level, skill AS combo, count(*)::BIGINT AS n FROM skill_rows GROUP BY skill
      ), pairs AS (
        SELECT 2 AS level, a.skill || ' + ' || b.skill AS combo, count(*)::BIGINT AS n
        FROM skill_rows a JOIN skill_rows b ON a.rid=b.rid AND a.skill < b.skill
        GROUP BY a.skill,b.skill
      ), triples AS (
        SELECT 3 AS level, a.skill || ' + ' || b.skill || ' + ' || c.skill AS combo, count(*)::BIGINT AS n
        FROM skill_rows a JOIN skill_rows b ON a.rid=b.rid AND a.skill < b.skill
        JOIN skill_rows c ON a.rid=c.rid AND b.skill < c.skill
        GROUP BY a.skill,b.skill,c.skill
      )
      SELECT level, combo, n FROM singles UNION ALL SELECT * FROM pairs UNION ALL SELECT * FROM triples
      ORDER BY level, combo
    """
    rows = con.execute(q).fetchall()
    selected_count = con.execute(f"""SELECT count(*) FROM read_csv('{uri}', header=true, all_varchar=true, nullstr='')
      WHERE contains(lower(coalesce(name,'')), 'python')
         OR list_contains(list_transform(string_split(coalesce(key_skills,''), ';'), x -> lower(trim(x))), 'python')""").fetchone()[0]
    counts = {"1": {}, "2": {}, "3": {}}
    for level, combo, n in rows:
        counts[str(level)][combo] = n
    # Independent salary check for the most frequent pairs/triples; salaries are RUB-only.
    wanted = [(level, name) for level in (2, 3)
              for name, _ in sorted(counts[str(level)].items(), key=lambda kv: (-kv[1], kv[0]))[:25]]
    impact = []
    if wanted:
        values = ",".join(f"({level}, '{name.replace("'", "''")}')" for level, name in wanted)
        iq = f"""
          WITH raw AS (
            SELECT row_number() OVER () AS rid, name, key_skills, currency, salary_from, salary_to
            FROM read_csv('{uri}', header=true, all_varchar=true, nullstr='')
          ), chosen AS (
            SELECT rid, key_skills,
              CASE WHEN upper(coalesce(currency,''))='RUR' THEN
                CASE WHEN try_cast(salary_from AS DOUBLE)>0 AND try_cast(salary_to AS DOUBLE)>0
                  THEN (try_cast(salary_from AS DOUBLE)+try_cast(salary_to AS DOUBLE))/2
                  ELSE coalesce(nullif(try_cast(salary_from AS DOUBLE),0), nullif(try_cast(salary_to AS DOUBLE),0)) END
              ELSE NULL END AS salary_rur
            FROM raw
            WHERE contains(lower(coalesce(name,'')), 'python') OR
              list_contains(list_transform(string_split(coalesce(key_skills,''), ';'), x -> lower(trim(x))), 'python')
          ), skills AS (
            SELECT DISTINCT c.rid, lower(trim(u.skill)) AS skill, c.salary_rur
            FROM chosen c, unnest(string_split(coalesce(c.key_skills,''), ';')) AS u(skill)
            WHERE trim(u.skill)<>''
          ), wanted(level, combo) AS (VALUES {values}),
          expanded AS (
            SELECT 2 AS level, a.skill || ' + ' || b.skill AS combo, a.rid, a.salary_rur
            FROM skills a JOIN skills b ON a.rid=b.rid AND a.skill<b.skill
            UNION ALL
            SELECT 3 AS level, a.skill || ' + ' || b.skill || ' + ' || c.skill AS combo, a.rid, a.salary_rur
            FROM skills a JOIN skills b ON a.rid=b.rid AND a.skill<b.skill
              JOIN skills c ON a.rid=c.rid AND b.skill<c.skill
          ), combos AS (
            SELECT e.level, e.combo, count(*) AS vacancy_count, median(e.salary_rur) AS median_salary_rur
            FROM expanded e JOIN wanted w ON e.level=w.level AND e.combo=w.combo
            GROUP BY e.level,e.combo
          ), singles AS (
            SELECT skill AS combo, median(salary_rur) AS median_salary_rur
            FROM skills GROUP BY skill
          )
          SELECT 'combo' AS kind, level, combo, vacancy_count, median_salary_rur FROM combos
          UNION ALL
          SELECT 'single' AS kind, 1 AS level, combo, 0 AS vacancy_count, median_salary_rur FROM singles
        """
        impact_rows = con.execute(iq).fetchall()
        medians = {name: sal for kind, _, name, _, sal in impact_rows if kind == "single"}
        combos_out = []
        for kind, level, name, n, median_salary in impact_rows:
            if kind != "combo":
                continue
            parts = name.split(" + ")
            baselines = [medians[p] for p in parts if medians.get(p) is not None]
            baseline = max(baselines) if baselines else None
            combos_out.append({"level": level, "combo": name, "vacancies": n,
                "median_salary_rur": median_salary, "best_constituent_single_median_rur": baseline,
                "premium_percent": round((median_salary / baseline - 1) * 100, 2) if median_salary and baseline else None})
        impact = sorted(combos_out, key=lambda x: (-1 if x["premium_percent"] is None else -x["premium_percent"]))
    result = {"engine": "DuckDB SQL over RustFS S3", "python_vacancies": selected_count,
        "counts": counts, "salary_impact_top_combinations": impact,
        "elapsed_seconds": round(time.perf_counter() - started, 3), "source": uri}
    emit_json(OUT / "duckdb.json", result)
    s3client().put_object(Bucket=BUCKET, Key="results/duckdb.json",
        Body=json.dumps(result, ensure_ascii=False).encode(), ContentType="application/json")
    con.close()
    return result


def audit():
    mr_paths = sorted(OUT.glob("mapreduce_*.json"))
    if not mr_paths:
        raise RuntimeError("No MapReduce outputs; run profile first")
    sql = json.loads((OUT / "duckdb.json").read_text(encoding="utf-8"))
    baseline = json.loads(mr_paths[0].read_text(encoding="utf-8"))
    report = {"status": "PASS", "python_vacancies_equal": baseline["python_vacancies"] == sql["python_vacancies"], "runs": []}
    for path in mr_paths:
        mr = json.loads(path.read_text(encoding="utf-8"))
        diff = {}
        for level in ("1", "2", "3"):
            a, b = mr["counts"][level], sql["counts"][level]
            keys = set(a) | set(b)
            unequal = {k: {"mapreduce": a.get(k, 0), "duckdb": b.get(k, 0)} for k in keys if a.get(k, 0) != b.get(k, 0)}
            diff[level] = {"equal": not unequal, "mismatch_count": len(unequal), "examples": dict(list(unequal.items())[:10])}
        run_equal = all(x["equal"] for x in diff.values())
        report["runs"].append({"chunk_rows": mr["chunk_rows"], "equal": run_equal, "levels": diff})
        report["status"] = "PASS" if report["status"] == "PASS" and run_equal else "FAIL"
    if not report["python_vacancies_equal"]:
        report["status"] = "FAIL"
    emit_json(OUT / "audit.json", report)
    s3client().put_object(Bucket=BUCKET, Key="results/audit.json", Body=json.dumps(report, indent=2).encode(), ContentType="application/json")
    print(json.dumps(report, indent=2))
    if report["status"] != "PASS":
        raise SystemExit(2)


def fail_test():
    try:
        map_chunk([{"name": "Python Engineer", "key_skills": "Python;SQL"}], fail_at=0, offset=0)
    except RuntimeError as exc:
        print(json.dumps({"status": "PASS", "expected_failure": str(exc), "published_result": False}, ensure_ascii=False))
        return
    raise SystemExit("FAIL: injected mapper failure was not observed")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["mr", "duckdb", "audit", "fail-test", "profile"])
    p.add_argument("--chunk-rows", type=int, default=10000)
    a = p.parse_args()
    if a.command == "mr":
        print(json.dumps(mr_run(a.chunk_rows), ensure_ascii=False))
    elif a.command == "duckdb":
        print(json.dumps(duckdb_run(), ensure_ascii=False))
    elif a.command == "audit":
        audit()
    elif a.command == "fail-test":
        fail_test()
    else:
        # Three stack sizes; the same full dataset and exact logic are used for every run.
        for size in (1000, 10000, 100000):
            proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "mr", "--chunk-rows", str(size)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            for line in proc.stdout:
                print(line, end="", flush=True)
            if proc.wait() != 0:
                raise SystemExit(f"MapReduce profiling run failed for chunk_rows={size}")


if __name__ == "__main__":
    main()
