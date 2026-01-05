import csv
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


# ======= EDIT THESE =======
NUM_RUNS = 10
BASE_DIR = Path(__file__).resolve().parent
FILE_PREFIX = "kafka_lag_run"   # run files: kafka_lag_run1.csv ... kafka_lag_run10.csv
STEP_SEC = 1.0                 # average every 1 second
OUT_CSV = BASE_DIR / "lag_mean_std.csv"
OUT_PNG = BASE_DIR / "lag_mean.png"
WARMUP_SEC = 10.0

RATE_EPS = 50  # input rate for this set of runs

# ==========================


def read_run(run_index: int):
    """
    Reads one CSV and returns two lists: times (t_sec) and lags (total_lag).
    """
    filename = BASE_DIR / f"{FILE_PREFIX}{run_index}.csv"
    times = []
    lags = []

    with open(filename, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = float(row["t_sec"])
            lag = float(row["total_lag"])
            times.append(t)
            lags.append(lag)

    # normalize so each run starts at t=0
    #also make warmup takes negative. later within interpolation negative times are ignored
    if times:
        t0 = times[0]
        times = [t - t0 for t in times]
        times = [t - WARMUP_SEC for t in times]

    return times, lags


def interpolate(times, lags, grid):
    """
    Interpolates lag values onto the common time grid.
    Outside the run's time range -> NaN (ignored in averaging).
    """
    #filter negative times
    pairs = [(t, l) for t, l in zip(times, lags) if t >= 0]
    if len(pairs) < 2:
        return np.full(len(grid), np.nan)

    times_np = np.array([p[0] for p in pairs])
    lags_np = np.array([p[1] for p in pairs])

    y = np.interp(grid, times_np, lags_np).astype(float)

    # mask outside range
    y[grid < times_np[0]] = np.nan
    y[grid > times_np[-1]] = np.nan
    return y


def main():
    # 1) Read all runs
    all_runs = []
    max_time = 0.0

    for i in range(1, NUM_RUNS + 1):
        times, lags = read_run(i)
        print(f"Loaded run {i}: {len(times)} points")
        all_runs.append((times, lags))
        if times:
            max_time = max(max_time, times[-1])

    # 2) Create a common time axis: 0, 1, 2, 3, ... (or STEP_SEC steps)
    grid = np.arange(0.0, max_time + 1e-9, STEP_SEC)

    # 3) Interpolate every run onto that grid
    Y = []
    for (times, lags) in all_runs:
        y = interpolate(times, lags, grid)
        Y.append(y)

    Y = np.vstack(Y)  # shape = (NUM_RUNS, len(grid))

    # 4) Compute mean and std at each time point (ignoring NaNs)
    mean = np.nanmean(Y, axis=0)
    std = np.nanstd(Y, axis=0)
    n = np.sum(~np.isnan(Y), axis=0)

    # 5) Save mean/std to CSV
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["t_sec", "mean_lag", "std_lag", "n_runs"])
        for t, m, s, nn in zip(grid, mean, std, n):
            writer.writerow([f"{t:.3f}", f"{m:.3f}", f"{s:.3f}", int(nn)])

    # 6) Plot the mean curve (+/- 1 std shaded)
    plt.figure()
    plt.plot(grid, mean, label="mean lag")

    mask = n >= 2
    plt.fill_between(grid[mask], (mean - std)[mask], (mean + std)[mask], alpha=0.2, label="±1 std")

    plt.xlabel("Time since start (s)")
    plt.ylabel("Total lag (messages)")
    plt.title(f"Average Kafka lag over {NUM_RUNS} runs @ {RATE_EPS} events/s (step={STEP_SEC}s)")
    plt.legend()
    plt.grid(True, alpha=0.2)
    plt.savefig(OUT_PNG, dpi=150, bbox_inches="tight")

    print("Saved:", OUT_CSV)
    print("Saved:", OUT_PNG)


if __name__ == "__main__":
    main()