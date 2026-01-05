import csv
import subprocess
import time
from pathlib import Path
import matplotlib.pyplot as plt

CONTAINER = "kafka200"
BROKER = "localhost:29092"
GROUP = "flink-sql-cart"
TOPIC = None  # e.g. "cart-events"

INTERVAL = 1          # seconds between polls
ZERO_STREAK = 5       # 0 lag for N consecutive polls
MAX_SECONDS = 60 * 30 # safety stop (30 min)


# change the path name from ..._run1 to run2 run 3 etc for each respective repetition of the experiment so the average plotter can read them and they dont overwrite.
BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "kafka_lag_run1.csv"
PLOT_PATH = BASE_DIR / "kafka_lag_run1.png"



def run_describe() -> str:
    cmd = [
        "docker", "exec", "-i", CONTAINER,
        "/usr/bin/kafka-consumer-groups",
        "--bootstrap-server", BROKER,
        "--describe",
        "--group", GROUP,
    ]
    if TOPIC:
        cmd += ["--topic", TOPIC]

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or "").strip() or f"Command failed: {p.returncode}")
    return (p.stdout or "").strip()


def parse_total_lag(describe_output: str) -> int:
    total = 0
    for ln in describe_output.splitlines():
        ln = ln.strip()
        if not ln or ln.upper().startswith("GROUP"):
            continue
        parts = ln.split()
        if len(parts) < 6:
            continue
        lag_str = parts[5]
        if lag_str in ("-", "unknown", "UNKNOWN"):
            continue
        total += int(lag_str)
    return total


def main():
    start_time = time.time()
    zero_count = 0
    max_lag = 0

    t_secs = []
    lags = []

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["t_sec", "total_lag"])

        while True:
            elapsed = time.time() - start_time
            if elapsed > MAX_SECONDS:
                print(f"Stopped after MAX_SECONDS={MAX_SECONDS} without reaching stable zero.")
                break

            try:
                out = run_describe()
                lag = parse_total_lag(out)
            except Exception as e:
                print(f"[t={elapsed:.3f}s] Error: {e}")
                lag = -1

            if lag >= 0:
                max_lag = max(max_lag, lag)
                
            t_secs.append(elapsed)
            lags.append(lag)
            writer.writerow([f"{elapsed:.3f}", lag])
            f.flush()

            print(f"[t={elapsed:.3f}s] total_lag={lag}")

            if lag == 0:
                zero_count += 1
            else:
                zero_count = 0

            if zero_count >= ZERO_STREAK:
                print(f"Lag reached 0 for {ZERO_STREAK} consecutive polls. Stopping.")
                print(f"Max recorded lag: {max_lag} messages")
                break

            time.sleep(INTERVAL)

    # Plot once at the end
    plt.figure()
    plt.plot(t_secs, lags)
    plt.xlabel("Time since start (s)")
    plt.ylabel("Total lag (sum of partitions)")
    title = f"Kafka lag until zero — group={GROUP}" + (f", topic={TOPIC}" if TOPIC else "")
    plt.title(title)
    plt.savefig(PLOT_PATH, dpi=150, bbox_inches="tight")

    print(f"Saved CSV: {CSV_PATH}")
    print(f"Saved plot: {PLOT_PATH}")


if __name__ == "__main__":
    main()