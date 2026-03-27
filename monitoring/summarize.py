#!/usr/bin/env python3
"""
summarize.py  —  Parse a collect_metrics.sh results directory and print
                 a full system-stability report covering:
                   1. Write throughput & latency percentiles
                   2. Queue depth over time  (RabbitMQ room.* + db.room.*)
                   3. Database performance metrics  (pg_stat_*)
                   4. Memory usage patterns  (JVM heap + PostgreSQL RSS)
                   5. Connection pool statistics  (HikariCP + pg_stat_activity)

Usage:
    python3 monitoring/summarize.py <results_dir>

Example:
    python3 monitoring/summarize.py monitoring/results/test1_baseline
"""
import json, sys, os
from statistics import mean, median

# ── helpers ────────────────────────────────────────────────────────────────

def load_jsonl(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return rows

def pct(values, p):
    if not values: return 0
    s = sorted(values)
    return s[max(0, int(len(s) * p / 100) - 1)]

def fmt_mem(usage_str):
    if not usage_str or "error" in str(usage_str): return "n/a"
    return str(usage_str).split("/")[0].strip()

def strip_pct(s):
    try: return float(str(s).replace("%","").strip())
    except: return 0.0

def stat_line(label, values, unit=""):
    if not values:
        print(f"  {label}: no data")
        return
    print(f"  {label}: min={min(values):.1f}{unit}  "
          f"avg={mean(values):.1f}{unit}  "
          f"max={max(values):.1f}{unit}")

def section(title):
    print(f"\n── {title} {'─'*(54-len(title))}")

# ── load files ─────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(1)

    run_dir = sys.argv[1]
    metrics = load_jsonl(os.path.join(run_dir, "metrics_log.jsonl"))
    docker  = load_jsonl(os.path.join(run_dir, "docker_stats.jsonl"))
    jvm_ps  = load_jsonl(os.path.join(run_dir, "jvm_stats.jsonl"))
    mq      = load_jsonl(os.path.join(run_dir, "rabbitmq_stats.jsonl"))

    if not metrics:
        print(f"No metrics_log.jsonl found in {run_dir}")
        print("Run  ./monitoring/collect_metrics.sh  during your load test first.")
        sys.exit(1)

    # ── parse metrics_log ─────────────────────────────────────────────────
    wps_series, p50_s, p95_s, p99_s = [], [], [], []
    buf_series, total_written = [], []
    hikari_active, hikari_idle = [], []
    qps_series, hit_ratio_s, lock_wait_s = [], [], []
    pg_conn_active = []
    jvm_heap_used, jvm_heap_pct, jvm_threads = [], [], []

    for row in metrics:
        d = row.get("data", {})
        if "error" in d: continue

        wm = d.get("writerMetrics", {})
        dm = d.get("dbMetrics", {})
        jm = d.get("jvmMetrics", {})

        # Write throughput
        v = wm.get("writesPerSecond")
        if isinstance(v, (int,float)) and v > 0: wps_series.append(float(v))

        # Latency percentiles
        for lst, key in [(p50_s,"p50WriteLatencyMs"),
                         (p95_s,"p95WriteLatencyMs"),
                         (p99_s,"p99WriteLatencyMs")]:
            v = wm.get(key)
            if isinstance(v,(int,float)): lst.append(float(v))

        # Buffer depth
        v = wm.get("bufferSize")
        if isinstance(v,(int,float)): buf_series.append(int(v))

        # Total written (last value = final count)
        v = wm.get("totalWritten")
        if isinstance(v,(int,float)): total_written.append(int(v))

        # HikariCP pool
        hp = dm.get("hikariPool", {})
        for lst, key in [(hikari_active,"active"),(hikari_idle,"idle")]:
            v = hp.get(key)
            if isinstance(v,(int,float)): lst.append(int(v))

        # pg_stat: transactions/sec
        qd = dm.get("queriesPerSecond", {})
        v = qd.get("transactionsPerSec")
        if isinstance(v,(int,float)) and v > 0: qps_series.append(float(v))

        # pg_stat: buffer hit ratio (interval, not cumulative)
        bhr = dm.get("bufferPoolHitRatio", {})
        v = bhr.get("intervalHitRatioPct")
        if isinstance(v,(int,float)) and v > 0: hit_ratio_s.append(float(v))

        # pg_stat: lock waits
        lw = dm.get("lockWaits", {})
        v = lw.get("waitingLocks")
        if isinstance(v,(int,float)): lock_wait_s.append(int(v))

        # pg_stat: active connections
        ac = dm.get("activeConnections", {})
        v = ac.get("active")
        if isinstance(v,(int,float)): pg_conn_active.append(int(v))

        # JVM heap (from /metrics endpoint)
        v = jm.get("heapUsedMb")
        if isinstance(v,(int,float)): jvm_heap_used.append(int(v))
        v = jm.get("heapUsedPct")
        if isinstance(v,(int,float)): jvm_heap_pct.append(float(v))
        v = jm.get("threadCount")
        if isinstance(v,(int,float)): jvm_threads.append(int(v))

    # ── parse docker_stats ────────────────────────────────────────────────
    pg_cpu, pg_memp, pg_mem_last = [], [], "n/a"
    for row in docker:
        st = row.get("stats", {})
        if "error" in st: continue
        pg_cpu.append(strip_pct(st.get("cpuPct",0)))
        pg_memp.append(strip_pct(st.get("memPct",0)))
        pg_mem_last = fmt_mem(st.get("memUsage",""))

    # ── parse jvm_stats (ps RSS) ──────────────────────────────────────────
    jvm_cpu_ps, jvm_rss = [], []
    for row in jvm_ps:
        j = row.get("jvm", {})
        if "error" in j: continue
        jvm_cpu_ps.append(strip_pct(j.get("cpuPct",0)))
        rss = j.get("rssKb")
        if isinstance(rss,(int,float)): jvm_rss.append(int(rss)//1024)

    # ── parse rabbitmq_stats ──────────────────────────────────────────────
    # queues dict: name → list of depth samples over time
    room_depths  = {}   # room.1 … room.20
    db_depths    = {}   # db.room.1 … db.room.20
    for row in mq:
        qs = row.get("queues", [])
        if not isinstance(qs, list): continue
        for q in qs:
            name  = q.get("name","")
            depth = q.get("depth", 0)
            if isinstance(depth, int):
                if name.startswith("db.room."):
                    db_depths.setdefault(name, []).append(depth)
                elif name.startswith("room."):
                    room_depths.setdefault(name, []).append(depth)

    # Aggregate across all queues of the same type
    all_room_depths = [d for lst in room_depths.values() for d in lst]
    all_db_depths   = [d for lst in db_depths.values()   for d in lst]

    # ── print report ──────────────────────────────────────────────────────
    run_label = os.path.basename(os.path.abspath(run_dir))
    print()
    print("=" * 60)
    print(f"  System Stability Report  —  {run_label}")
    print("=" * 60)

    # ── 1. Write throughput ───────────────────────────────────────────────
    section("Write Throughput  (messages/sec)")
    if wps_series:
        print(f"  Max          : {max(wps_series):.0f}")
        print(f"  Avg (mean)   : {mean(wps_series):.0f}")
        print(f"  Median       : {median(wps_series):.0f}")
        print(f"  Min          : {min(wps_series):.0f}")
        if total_written:
            print(f"  Total written: {total_written[-1]:,}")
    else:
        print("  No data")

    section("Write Batch Latency  (ms per batch)")
    for label, series in [("p50", p50_s), ("p95", p95_s), ("p99", p99_s)]:
        if series:
            print(f"  {label}: min={min(series):.0f}  "
                  f"avg={mean(series):.1f}  "
                  f"p{label[1:]}={pct(series,int(label[1:])):.0f}  "
                  f"max={max(series):.0f}")
        else:
            print(f"  {label}: no data")

    # ── 2. Queue depth ────────────────────────────────────────────────────
    section("Queue Depth Over Time  (RabbitMQ)")
    if all_room_depths:
        print(f"  room.*  queues  max={max(all_room_depths)}  "
              f"avg={mean(all_room_depths):.0f}  "
              f"p99={pct(all_room_depths,99)}")
        # Per-queue max depth table
        worst = sorted(room_depths.items(), key=lambda kv: max(kv[1]), reverse=True)[:5]
        if worst:
            print("  Top 5 deepest room queues:")
            for name, depths in worst:
                print(f"    {name:<18} max={max(depths):>6}  avg={mean(depths):>6.0f}")
    else:
        print("  room.*  : no data  (is rabbitmq_stats.jsonl present?)")

    if all_db_depths:
        print(f"  db.room.* queues  max={max(all_db_depths)}  "
              f"avg={mean(all_db_depths):.0f}  "
              f"p99={pct(all_db_depths,99)}")
    else:
        print("  db.room.* : no data")

    if all_room_depths or all_db_depths:
        depth_threshold = 1000
        high_room = sum(1 for d in all_room_depths if d > depth_threshold)
        high_db   = sum(1 for d in all_db_depths   if d > depth_threshold)
        total_samples = max(len(all_room_depths), 1)
        print(f"  Samples with depth > {depth_threshold}: "
              f"room={high_room}/{len(all_room_depths)}  "
              f"db={high_db}/{len(all_db_depths)}")

    # ── 3. Database performance metrics ───────────────────────────────────
    section("Database Performance  (pg_stat_*)")
    stat_line("Transactions/sec", qps_series, " tps")

    if hit_ratio_s:
        print(f"  Buffer hit ratio (interval): "
              f"min={min(hit_ratio_s):.2f}%  "
              f"avg={mean(hit_ratio_s):.2f}%  "
              f"max={max(hit_ratio_s):.2f}%")
        below99 = sum(1 for r in hit_ratio_s if r < 99)
        print(f"  Samples below 99%: {below99}/{len(hit_ratio_s)}"
              + ("  ⚠ disk pressure" if below99 > len(hit_ratio_s)*0.1 else ""))
    else:
        print("  Buffer hit ratio: no data")

    if lock_wait_s:
        non_zero = sum(1 for x in lock_wait_s if x > 0)
        print(f"  Lock waits: max={max(lock_wait_s)}  "
              f"avg={mean(lock_wait_s):.2f}  "
              f"samples_with_waits={non_zero}/{len(lock_wait_s)}")
    else:
        print("  Lock waits: no data")

    section("PostgreSQL Container  (docker stats)")
    stat_line("CPU", pg_cpu, "%")
    if pg_memp: stat_line("Memory %", pg_memp, "%")
    if pg_mem_last != "n/a": print(f"  Memory usage (last sample): {pg_mem_last}")

    # ── 4. Memory usage patterns ──────────────────────────────────────────
    section("Memory Usage Patterns  (db-server JVM)")
    if jvm_heap_used:
        first, last = jvm_heap_used[0], jvm_heap_used[-1]
        print(f"  Heap used  : min={min(jvm_heap_used)} MB  "
              f"avg={mean(jvm_heap_used):.0f} MB  "
              f"max={max(jvm_heap_used)} MB  "
              f"delta={last-first:+d} MB")
        if last - first > 100:
            print("  ⚠  Heap grew > 100 MB — possible memory leak")
    else:
        print("  Heap: no data (JVM metrics in /metrics endpoint)")

    if jvm_heap_pct:
        print(f"  Heap used% : min={min(jvm_heap_pct):.1f}%  "
              f"avg={mean(jvm_heap_pct):.1f}%  "
              f"max={max(jvm_heap_pct):.1f}%")

    if jvm_threads:
        print(f"  Threads    : min={min(jvm_threads)}  "
              f"avg={mean(jvm_threads):.0f}  "
              f"max={max(jvm_threads)}")

    if jvm_rss:
        print(f"  RSS (ps)   : min={min(jvm_rss)} MB  "
              f"avg={mean(jvm_rss):.0f} MB  "
              f"max={max(jvm_rss)} MB  "
              f"delta={jvm_rss[-1]-jvm_rss[0]:+d} MB")

    stat_line("CPU (ps)", jvm_cpu_ps, "%")

    # ── 5. Connection pool statistics ─────────────────────────────────────
    section("Connection Pool Statistics")
    print("  HikariCP (db-server pool):")
    stat_line("    Active connections", [float(x) for x in hikari_active])
    stat_line("    Idle connections",   [float(x) for x in hikari_idle])

    print("  PostgreSQL (pg_stat_activity):")
    stat_line("    Active sessions", [float(x) for x in pg_conn_active])

    # ── summary ───────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print(f"  {len(metrics)} metrics samples  •  {len(mq)} RabbitMQ samples")
    print(f"  Raw data: {os.path.abspath(run_dir)}/")
    print("=" * 60)
    print()

if __name__ == "__main__":
    main()
