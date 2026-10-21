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
    access = os.getenv("RUSTFS_ACCESS_KEY")
    secret = os.getenv("RUSTFS_SECRET_KEY")
    if not access or not secret:
        raise RuntimeError("Set RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY in the environment")
    return boto3.client("s3", endpoint_url=ENDPOINT,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1", config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))


def skills_of(raw: str) -> tuple[str, ...]:
    return tuple(sorted({s.strip().lower() for s in (raw or "").split(";") if s.strip()}))


def selected(row: dict[str, str]) -> bool:
    # Variant = Python. Exact skill token OR title substring; description is deliberately excluded.
    if "python" in (row.get("name") or "").lower():
        return True
    return any(token.strip().lower() == "python" for token in (row.get("key_skills") or "").split(";"))


def combos(skills: tuple[str, ...], n: int):
    return (" + ".join(c) for c in itertools.combinations(skills, n))


def emit_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def map_chunk(rows: list[dict[str, str]], fail_at: int | None = None, offset: int = 0):
    counts = [collections.Counter() for _ in range(3)]
    regions = {}
    skill_histogram = collections.Counter()
    contributions = [0, 0, 0]
    if fail_at is not None and offset <= fail_at < offset + len(rows):
        raise RuntimeError(f"injected mapper failure at input row {fail_at + 1}")
    selected_rows = 0
    for row in rows:
        if not selected(row):
            continue
        selected_rows += 1
        skills = skills_of(row.get("key_skills", ""))
        k = len(skills)
        skill_histogram[k] += 1
        contributions[0] += k
        contributions[1] += k * (k - 1) // 2
        contributions[2] += k * (k - 1) * (k - 2) // 6
        area = (row.get("area") or "").strip() or "(не указан)"
        region = regions.setdefault(area, {"vacancies": 0, "salary_rur": [],
            "skill_counts": [collections.Counter() for _ in range(3)]})
        region["vacancies"] += 1
        for n in (1, 2, 3):
            combo_counts = collections.Counter(combos(skills, n))
            counts[n - 1].update(combo_counts)
            region["skill_counts"][n - 1].update(combo_counts)
        if (row.get("currency") or "").upper() == "RUR":
            try:
                low = float(row.get("salary_from") or 0)
                high = float(row.get("salary_to") or 0)
                salary = (low + high) / 2 if low > 0 and high > 0 else low or high
                if salary > 0:
                    region["salary_rur"].append(salary)
            except ValueError:
                pass
    return selected_rows, counts, regions, skill_histogram, contributions


def mr_run(chunk_rows: int, fail_at: int | None = None, persist: bool = True):
    started = time.perf_counter()
    s3 = s3client()
    body = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"]
    text_stream = io.TextIOWrapper(gzip.GzipFile(fileobj=body), encoding="utf-8", newline="")
    reader = csv.DictReader(text_stream)
    total = selected_total = 0
    reduced = [collections.Counter() for _ in range(3)]
    regions = {}
    skill_histogram = collections.Counter()
    contributions = [0, 0, 0]
    while True:
        rows = list(itertools.islice(reader, chunk_rows))
        if not rows:
            break
        mapped, local, local_regions, local_histogram, local_contributions = map_chunk(rows, fail_at=fail_at, offset=total)
        total += len(rows)
        selected_total += mapped
        for i in range(3):
            reduced[i].update(local[i])
            contributions[i] += local_contributions[i]
        skill_histogram.update(local_histogram)
        for area, part in local_regions.items():
            target = regions.setdefault(area, {"vacancies": 0, "salary_rur": [],
                "skill_counts": [collections.Counter() for _ in range(3)]})
            target["vacancies"] += part["vacancies"]
            target["salary_rur"].extend(part["salary_rur"])
            for i in range(3):
                target["skill_counts"][i].update(part["skill_counts"][i])
        if total % (chunk_rows * 10) == 0:
            print(f"read_rows={total} selected={selected_total}", flush=True)
    result = {
        "engine": "custom-streaming-mapreduce", "chunk_rows": chunk_rows,
        "input_rows": total, "python_vacancies": selected_total,
        "skill_count_histogram": {str(k): v for k,v in sorted(skill_histogram.items())},
        "combination_contribution_totals": {"1": contributions[0], "2": contributions[1], "3": contributions[2]},
        "counts": {str(i + 1): dict(sorted(reduced[i].items())) for i in range(3)},
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "source": f"s3://{BUCKET}/{KEY}",
        "filter": "case-insensitive 'python' substring in name OR exact semicolon-delimited key_skills token 'python'",
        "top_regions": [{"area": area, "vacancies": item["vacancies"],
            "salary_observations": len(item["salary_rur"]),
            "median_salary_rur": round(statistics.median(item["salary_rur"]), 2) if item["salary_rur"] else None,
            "top_skills": [{"combo": name, "vacancies": count} for name, count in item["skill_counts"][0].most_common(10)],
            "top_pairs": [{"combo": name, "vacancies": count} for name, count in item["skill_counts"][1].most_common(10)],
            "top_triples": [{"combo": name, "vacancies": count} for name, count in item["skill_counts"][2].most_common(10)]}
            for area, item in sorted(regions.items(), key=lambda kv: (-kv[1]["vacancies"],kv[0]))[:3]],
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
    access = os.getenv("RUSTFS_ACCESS_KEY", "").replace("'", "''")
    secret = os.getenv("RUSTFS_SECRET_KEY", "").replace("'", "''")
    if not access or not secret:
        raise RuntimeError("Set RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY in the environment")
    endpoint = ENDPOINT.removeprefix("http://").removeprefix("https://").replace("'", "''")
    ssl = "true" if ENDPOINT.startswith("https://") else "false"
    con.execute(f"""CREATE SECRET rustfs_secret (TYPE s3, PROVIDER config,
        KEY_ID '{access}', SECRET '{secret}', REGION 'us-east-1',
        ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL {ssl})""")
    uri = f"s3://{BUCKET}/{KEY}"
    # One S3 scan materializes only the selected segment; subsequent SQL uses local temp tables.
    con.execute(f"""CREATE TEMP TABLE chosen AS
      SELECT row_number() OVER () AS rid,
        coalesce(nullif(trim(area),''),'(не указан)') AS area,
        CASE WHEN upper(coalesce(currency,''))='RUR' THEN
          CASE WHEN try_cast(salary_from AS DOUBLE)>0 AND try_cast(salary_to AS DOUBLE)>0
            THEN (try_cast(salary_from AS DOUBLE)+try_cast(salary_to AS DOUBLE))/2
            ELSE coalesce(nullif(try_cast(salary_from AS DOUBLE),0),nullif(try_cast(salary_to AS DOUBLE),0)) END
          ELSE NULL END AS salary_rur,
        key_skills
      FROM read_csv('{uri}', header=true, all_varchar=true, nullstr='')
      WHERE contains(lower(coalesce(name,'')), 'python') OR
        list_contains(list_transform(string_split(coalesce(key_skills,''), ';'), x -> lower(trim(x))), 'python')""")
    selected_count = con.execute("SELECT count(*) FROM chosen").fetchone()[0]
    con.execute("""CREATE TEMP TABLE skill_rows AS
      SELECT DISTINCT c.rid,c.area,c.salary_rur,lower(trim(u.skill)) AS skill
      FROM chosen c,unnest(string_split(coalesce(c.key_skills,''),';')) AS u(skill)
      WHERE trim(u.skill)<>''""")
    con.execute("""CREATE TEMP TABLE pair_rows AS
      SELECT a.rid,a.area,a.salary_rur,a.skill || ' + ' || b.skill AS combo
      FROM skill_rows a JOIN skill_rows b ON a.rid=b.rid AND a.skill<b.skill""")
    con.execute("""CREATE TEMP TABLE triple_rows AS
      SELECT a.rid,a.area,a.salary_rur,a.skill || ' + ' || b.skill || ' + ' || c.skill AS combo
      FROM skill_rows a JOIN skill_rows b ON a.rid=b.rid AND a.skill<b.skill
      JOIN skill_rows c ON a.rid=c.rid AND b.skill<c.skill""")
    counts = {"1": {}, "2": {}, "3": {}}
    sources = [("1", "skill", "skill_rows"), ("2", "combo", "pair_rows"), ("3", "combo", "triple_rows")]
    for level, col, table in sources:
        for name, n in con.execute(f"SELECT {col},count(*) FROM {table} GROUP BY 1").fetchall():
            counts[level][name] = n
    singles = {name: {"count": n, "median": med} for name,n,med in con.execute(
        "SELECT skill,count(*),median(salary_rur) FROM skill_rows GROUP BY skill").fetchall()}
    histogram = {str(k): n for k,n in con.execute("""WITH per_vacancy AS (
        SELECT c.rid,count(s.skill) AS k FROM chosen c LEFT JOIN skill_rows s ON c.rid=s.rid GROUP BY c.rid
      ) SELECT k,count(*) FROM per_vacancy GROUP BY k ORDER BY k""").fetchall()}
    histogram_counts = {"1":0,"2":0,"3":0}
    for k_text,n in histogram.items():
        k=int(k_text)
        histogram_counts["1"] += k*n
        histogram_counts["2"] += k*(k-1)//2*n
        histogram_counts["3"] += k*(k-1)*(k-2)//6*n
    salary_rows = []
    for level, table in ((2,"pair_rows"),(3,"triple_rows")):
        salary_rows.extend((level, name, n, salary_n, median) for name,n,salary_n,median in con.execute(
            f"SELECT combo,count(*),count(salary_rur),median(salary_rur) FROM {table} GROUP BY combo HAVING count(*)>=30 AND count(salary_rur)>=20").fetchall())
    impact = []
    for level,name,n,salary_n,median_salary in salary_rows:
        constituent = sorted((part for part in name.split(" + ") if part in singles),
            key=lambda skill:(-singles[skill]["count"],skill))
        baseline_skill = constituent[0] if constituent else None
        baseline = singles[baseline_skill]["median"] if baseline_skill else None
        premium = (median_salary / baseline - 1) * 100 if median_salary and baseline else None
        impact.append({"level":level,"combo":name,"vacancies":n,"salary_observations":salary_n,
            "median_salary_rur":median_salary,"baseline_skill_by_segment_frequency":baseline_skill,
            "baseline_skill_vacancies":singles[baseline_skill]["count"] if baseline_skill else None,
            "baseline_median_salary_rur":baseline,"premium_percent":round(premium,2) if premium is not None else None})
    ups = sorted((x for x in impact if x["premium_percent"] is not None),key=lambda x:(-x["premium_percent"],-x["vacancies"]))[:10]
    downs = sorted((x for x in impact if x["premium_percent"] is not None),key=lambda x:(x["premium_percent"],-x["vacancies"]))[:10]
    region_counts = con.execute("SELECT area,count(*),count(salary_rur),median(salary_rur) FROM chosen GROUP BY area ORDER BY count(*) DESC,area LIMIT 3").fetchall()
    regions = []
    for area,n,salary_n,median in region_counts:
        by_level = {}
        for level,table,col in (("1","skill_rows","skill"),("2","pair_rows","combo"),("3","triple_rows","combo")):
            by_level[level] = [{"combo":k,"vacancies":v} for k,v in con.execute(
                f"SELECT {col},count(*) AS n FROM {table} WHERE area=? GROUP BY 1 ORDER BY n DESC,{col} LIMIT 10",[area]).fetchall()]
        regions.append({"area":area,"python_vacancies":n,"salary_observations":salary_n,
            "median_salary_rur":median,"top_skills":by_level["1"],"top_pairs":by_level["2"],"top_triples":by_level["3"]})
    result = {"engine": "DuckDB SQL over RustFS S3", "python_vacancies": selected_count,
        "skill_count_histogram":histogram,"combination_contribution_totals":histogram_counts,
        "counts": counts, "single_skill_salary_baselines":singles,
        "salary_impact_candidates":impact,"top_salary_increases":ups,"top_salary_decreases":downs,
        "top_regions":regions,"elapsed_seconds":round(time.perf_counter()-started,3),
        "peak_rss_kib":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,"source":uri}
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
    report["combination_contribution_totals_equal"] = baseline.get("combination_contribution_totals") == sql.get("combination_contribution_totals")
    for path in mr_paths:
        mr = json.loads(path.read_text(encoding="utf-8"))
        diff = {}
        for level in ("1", "2", "3"):
            a, b = mr["counts"][level], sql["counts"][level]
            keys = set(a) | set(b)
            unequal = {k: {"mapreduce": a.get(k, 0), "duckdb": b.get(k, 0)} for k in keys if a.get(k, 0) != b.get(k, 0)}
            diff[level] = {"equal": not unequal, "mismatch_count": len(unequal), "examples": dict(list(unequal.items())[:10])}
        vacancy_equal = mr["python_vacancies"] == sql["python_vacancies"]
        histogram_equal = mr.get("skill_count_histogram") == sql.get("skill_count_histogram")
        contribution_equal = mr.get("combination_contribution_totals") == sql.get("combination_contribution_totals")
        run_equal = all(x["equal"] for x in diff.values()) and vacancy_equal and histogram_equal and contribution_equal
        sql_regions = {x["area"]:(x["python_vacancies"],x["salary_observations"],round(x["median_salary_rur"],2) if x["median_salary_rur"] is not None else None) for x in sql.get("top_regions",[])}
        mr_regions = {x["area"]:(x["vacancies"],x["salary_observations"],x["median_salary_rur"]) for x in mr.get("top_regions",[])}
        regions_equal = mr_regions == sql_regions
        run_equal = run_equal and regions_equal
        report["runs"].append({"chunk_rows": mr["chunk_rows"], "equal": run_equal,
            "python_vacancies_equal": vacancy_equal,"skill_histogram_equal":histogram_equal,
            "combination_contributions_equal":contribution_equal,"top_region_summary_equal":regions_equal,"levels": diff})
        report["status"] = "PASS" if report["status"] == "PASS" and run_equal else "FAIL"
    if not report["python_vacancies_equal"]:
        report["status"] = "FAIL"
    if not report["combination_contribution_totals_equal"]:
        report["status"] = "FAIL"
    emit_json(OUT / "audit.json", report)
    s3client().put_object(Bucket=BUCKET, Key="results/audit.json", Body=json.dumps(report, indent=2).encode(), ContentType="application/json")
    print(json.dumps(report, indent=2))
    if report["status"] != "PASS":
        raise SystemExit(2)


def fail_test():
    before = {p.name for p in OUT.glob("*.json")}
    try:
        # Reads a real S3 chunk; injected mapper error must abort before any result is published.
        mr_run(chunk_rows=10000, fail_at=0, persist=False)
    except RuntimeError as exc:
        after = {p.name for p in OUT.glob("*.json")}
        if after != before:
            raise RuntimeError("failure test wrote a partial local result")
        print(json.dumps({"status": "PASS", "expected_failure": str(exc), "published_result": False,
            "source": f"s3://{BUCKET}/{KEY}", "first_chunk_rows_read": 10000}, ensure_ascii=False))
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
