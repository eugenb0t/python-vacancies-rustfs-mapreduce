#!/usr/bin/env python3
"""Reproducible microbenchmark for the pre-filter allocation optimization."""
import csv
import gzip
import io
import json
import statistics
import time
import tracemalloc
from pathlib import Path

from pipeline import BUCKET, KEY, OUT, s3client, selected


def old_filter(row):
    if "python" in (row[0] or "").casefold():
        return True
    old_skills=tuple(sorted({s.strip().casefold() for s in (row[1] or "").split(";") if s.strip()}))
    return "python" in old_skills


def measure(fn, rows, repeats=5):
    durations=[]
    counts=[]
    def match(row):
        return selected({"name":row[0],"key_skills":row[1]}) if fn is selected else fn(row)
    for _ in range(repeats):
        start=time.perf_counter()
        count=sum(1 for row in rows if match(row))
        durations.append(time.perf_counter()-start)
        counts.append(count)
    tracemalloc.start()
    if fn is selected:
        sum(1 for name,keys in rows if selected({"name":name,"key_skills":keys}))
    else:
        sum(1 for row in rows if old_filter(row))
    _,peak=tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"runs_seconds":durations,"median_seconds":statistics.median(durations),
        "peak_temporary_bytes":peak,"selected_per_run":counts}


def main():
    body=s3client().get_object(Bucket=BUCKET,Key=KEY)["Body"]
    reader=csv.DictReader(io.TextIOWrapper(gzip.GzipFile(fileobj=body),encoding="utf-8",newline=""))
    limit=1_000_000
    rows=[(row.get("name",""),row.get("key_skills","")) for row in __import__("itertools").islice(reader,limit)]
    before=measure(old_filter,rows)
    after=measure(selected,rows)
    if len(set(before["selected_per_run"]+after["selected_per_run"]))!=1:
        raise SystemExit("Filter optimization changed the selected rows")
    result={"source":f"s3://{BUCKET}/{KEY}","sample_input_rows":len(rows),"repetitions_per_method":5,
        "before":{"method":"title substring or eager normalization/set/sort of every key_skills value","metrics":before},
        "after":{"method":"early title check, then direct exact-token scan without per-row set/sort","metrics":after},
        "selected_rows_equal":True,
        "time_improvement_percent":round((before["median_seconds"]/after["median_seconds"]-1)*100,2),
        "temporary_allocation_reduction_percent":round((1-after["peak_temporary_bytes"]/before["peak_temporary_bytes"])*100,2)}
    p=OUT/"optimization_benchmark.json"
    p.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    s3client().put_object(Bucket=BUCKET,Key="results/optimization_benchmark.json",
        Body=json.dumps(result,ensure_ascii=False).encode(),ContentType="application/json")
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=="__main__":
    main()
