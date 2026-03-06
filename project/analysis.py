import csv
import math
from datetime import datetime
from pathlib import Path
from statistics import mean

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError:
    plt = None


BASE_DIR = Path("/Users/tangyuchen/Desktop/cornell/26Spring/SYSEN5380/project")
INPUT_CSV = BASE_DIR / "polymarket_markets.csv"
OUTPUT_CSV = BASE_DIR / "polymarket_markets_with_variance.csv"
SCATTER_PLOT = BASE_DIR / "variance_vs_volume_scatter.png"
TIME_PLOT = BASE_DIR / "variance_over_time_with_volume.png"
BINNED_PLOT = BASE_DIR / "variance_by_volume_bin.png"
BOXPLOT = BASE_DIR / "variance_boxplot_by_volume_bin.png"
LOG_BINNED_SCATTER = BASE_DIR / "variance_vs_log_volume_binned.png"
ACCURACY_PLOT = BASE_DIR / "accuracy_by_volume_bin.png"
HIGH_CONFIDENCE_ACCURACY_PLOT = BASE_DIR / "high_confidence_accuracy_by_volume_bin.png"
MIDRANGE_ACCURACY_PLOT = BASE_DIR / "accuracy_by_volume_bin_midrange_p.png"


def parse_float(value):
    return float(value) if value not in ("", None) else None


def parse_int(value):
    return int(value) if value not in ("", None) else None


def parse_datetime(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def get_volume_bin(volume):
    if volume < 1_000:
        return "<1k"
    if volume < 10_000:
        return "1k-10k"
    if volume < 100_000:
        return "10k-100k"
    if volume < 1_000_000:
        return "100k-1m"
    return ">=1m"


def load_rows():
    rows = []
    with INPUT_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pred_prob = parse_float(row["pred_prob_day_minus_1"])
            final_outcome = parse_int(row["final_outcome_yes"])
            volume = parse_float(row["volume"])

            if pred_prob is None or final_outcome is None or volume is None:
                continue

            row["pred_prob_day_minus_1"] = pred_prob
            row["final_outcome_yes"] = final_outcome
            row["volume"] = volume
            row["endDate_dt"] = parse_datetime(row["endDate"])
            row["variance"] = (pred_prob - final_outcome) ** 2
            row["predicted_yes"] = 1 if pred_prob >= 0.5 else 0
            row["is_correct"] = 1 if row["predicted_yes"] == final_outcome else 0
            if pred_prob >= 0.6:
                row["high_conf_prediction"] = 1
                row["high_conf_is_correct"] = 1 if final_outcome == 1 else 0
            elif pred_prob <= 0.4:
                row["high_conf_prediction"] = 0
                row["high_conf_is_correct"] = 1 if final_outcome == 0 else 0
            else:
                row["high_conf_prediction"] = None
                row["high_conf_is_correct"] = None
            row["volume_bin"] = get_volume_bin(volume)
            rows.append(row)
    return rows


def write_output_csv(rows):
    fieldnames = [
        "market_id",
        "question",
        "category",
        "startDate",
        "endDate",
        "pred_prob_day_minus_1",
        "pred_prob_timestamp_utc",
        "final_outcome_yes",
        "volume",
        "liquidity",
        "closed",
        "resolution",
        "winner",
        "variance",
    ]

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            output_row = {name: row.get(name, "") for name in fieldnames}
            writer.writerow(output_row)


def plot_variance_vs_volume(rows):
    if plt is None:
        return False

    volumes = [row["volume"] for row in rows]
    variances = [row["variance"] for row in rows]

    plt.figure(figsize=(10, 6))
    plt.scatter(volumes, variances, alpha=0.35, s=16, edgecolors="none")
    plt.xscale("log")
    plt.xlabel("Volume (log scale)")
    plt.ylabel("Variance")
    plt.title("Variance vs. Volume")
    plt.grid(True, alpha=0.25)
    plt.tight_layout()
    plt.savefig(SCATTER_PLOT, dpi=200)
    plt.close()
    return True


def plot_variance_over_time(rows):
    if plt is None:
        return False

    ordered_rows = sorted(rows, key=lambda row: row["endDate_dt"])
    dates = [row["endDate_dt"] for row in ordered_rows]
    variances = [row["variance"] for row in ordered_rows]
    volumes = [row["volume"] for row in ordered_rows]
    marker_sizes = [18 + 10 * (volume > 1_000) + 18 * (volume > 10_000) for volume in volumes]

    plt.figure(figsize=(12, 6))
    scatter = plt.scatter(
        dates,
        variances,
        c=volumes,
        s=marker_sizes,
        cmap="viridis",
        alpha=0.65,
        edgecolors="none",
    )
    colorbar = plt.colorbar(scatter)
    colorbar.set_label("Volume")
    plt.xlabel("Event End Date")
    plt.ylabel("Variance")
    plt.title("Variance Over Time (colored by Volume)")
    plt.grid(True, alpha=0.25)
    plt.tight_layout()
    plt.savefig(TIME_PLOT, dpi=200)
    plt.close()
    return True


def summarize_volume_bins(rows):
    bin_order = ["<1k", "1k-10k", "10k-100k", "100k-1m", ">=1m"]
    grouped = {
        bin_name: {"variances": [], "volumes": [], "correct": []}
        for bin_name in bin_order
    }
    for row in rows:
        grouped[row["volume_bin"]]["variances"].append(row["variance"])
        grouped[row["volume_bin"]]["volumes"].append(row["volume"])
        grouped[row["volume_bin"]]["correct"].append(row["is_correct"])
        grouped[row["volume_bin"]].setdefault("high_conf_correct", [])
        if row["high_conf_is_correct"] is not None:
            grouped[row["volume_bin"]]["high_conf_correct"].append(row["high_conf_is_correct"])

    summary = []
    for bin_name in bin_order:
        variances = grouped[bin_name]["variances"]
        if not variances:
            continue
        summary.append(
            {
                "volume_bin": bin_name,
                "count": len(variances),
                "avg_variance": mean(variances),
                "variances": variances,
                "avg_volume": mean(grouped[bin_name]["volumes"]),
                "accuracy": mean(grouped[bin_name]["correct"]),
                "high_conf_count": len(grouped[bin_name]["high_conf_correct"]),
                "high_conf_accuracy": (
                    mean(grouped[bin_name]["high_conf_correct"])
                    if grouped[bin_name]["high_conf_correct"]
                    else None
                ),
            }
        )
    return summary


def build_log_volume_bins(rows, num_bins=20):
    positive_rows = [row for row in rows if row["volume"] > 0]
    if not positive_rows:
        return []

    ordered = sorted(positive_rows, key=lambda row: math.log10(row["volume"]))
    bins = []
    for i in range(num_bins):
        start = i * len(ordered) // num_bins
        end = (i + 1) * len(ordered) // num_bins
        chunk = ordered[start:end]
        if not chunk:
            continue
        log_volumes = [math.log10(row["volume"]) for row in chunk]
        variances = [row["variance"] for row in chunk]
        bins.append(
            {
                "count": len(chunk),
                "avg_log_volume": mean(log_volumes),
                "avg_variance": mean(variances),
            }
        )
    return bins


def summarize_volume_bins_for_subset(rows):
    subset = [row for row in rows if 0.05 < row["pred_prob_day_minus_1"] < 0.95]
    return summarize_volume_bins(subset), subset


def plot_variance_by_volume_bin(bin_summary):
    if plt is None:
        return False

    labels = [row["volume_bin"] for row in bin_summary]
    avg_variances = [row["avg_variance"] for row in bin_summary]

    plt.figure(figsize=(9, 6))
    bars = plt.bar(labels, avg_variances, color=["#8ecae6", "#219ebc", "#ffb703", "#fb8500", "#023047"])
    plt.xlabel("Volume Bin")
    plt.ylabel("Average Variance")
    plt.title("Average Variance by Volume Bin")
    plt.grid(True, axis="y", alpha=0.25)
    for bar, row in zip(bars, bin_summary):
        x = bar.get_x() + bar.get_width() / 2
        y = row["avg_variance"]
        plt.text(x, y + 0.002, f"{y:.3f}", ha="center", va="bottom")
        plt.text(x, max(y * 0.55, 0.003), f"n={row['count']}", ha="center", va="center", color="white", fontsize=9)
    plt.tight_layout()
    plt.savefig(BINNED_PLOT, dpi=200)
    plt.close()
    return True


def plot_variance_boxplot_by_volume_bin(bin_summary):
    if plt is None:
        return False

    labels = [f"{row['volume_bin']}\n(n={row['count']})" for row in bin_summary]
    data = [row["variances"] for row in bin_summary]

    plt.figure(figsize=(10, 6))
    plt.boxplot(data, tick_labels=labels, showfliers=False)
    plt.xlabel("Volume Bin")
    plt.ylabel("Variance")
    plt.title("Variance Distribution by Volume Bin")
    plt.grid(True, axis="y", alpha=0.25)
    plt.tight_layout()
    plt.savefig(BOXPLOT, dpi=200)
    plt.close()
    return True


def plot_variance_vs_log_volume_binned(log_bin_summary):
    if plt is None or not log_bin_summary:
        return False

    x = [row["avg_log_volume"] for row in log_bin_summary]
    y = [row["avg_variance"] for row in log_bin_summary]
    sizes = [40 + row["count"] * 0.08 for row in log_bin_summary]

    plt.figure(figsize=(10, 6))
    plt.plot(x, y, color="#1d3557", linewidth=1.5, alpha=0.8)
    plt.scatter(x, y, s=sizes, color="#e76f51", alpha=0.8)
    plt.xlabel("Average log10(Volume) within Bin")
    plt.ylabel("Average Variance")
    plt.title("Binned Relationship Between log10(Volume) and Variance")
    plt.grid(True, alpha=0.25)
    plt.tight_layout()
    plt.savefig(LOG_BINNED_SCATTER, dpi=200)
    plt.close()
    return True


def plot_accuracy_by_volume_bin(bin_summary):
    if plt is None:
        return False

    labels = [row["volume_bin"] for row in bin_summary]
    accuracies = [row["accuracy"] for row in bin_summary]

    plt.figure(figsize=(9, 6))
    bars = plt.bar(labels, accuracies, color=["#577590", "#43aa8b", "#90be6d", "#f9c74f", "#f8961e"])
    plt.xlabel("Volume Bin")
    plt.ylabel("Prediction Accuracy")
    plt.ylim(0, 1.0)
    plt.title("Prediction Accuracy by Volume Bin")
    plt.grid(True, axis="y", alpha=0.25)
    for bar, row in zip(bars, bin_summary):
        x = bar.get_x() + bar.get_width() / 2
        y = row["accuracy"]
        plt.text(x, y + 0.02, f"{y:.3f}", ha="center", va="bottom")
        plt.text(x, max(y * 0.5, 0.06), f"n={row['count']}", ha="center", va="center", color="white", fontsize=9)
    plt.tight_layout()
    plt.savefig(ACCURACY_PLOT, dpi=200)
    plt.close()
    return True


def plot_high_confidence_accuracy_by_volume_bin(bin_summary):
    if plt is None:
        return False

    filtered = [row for row in bin_summary if row["high_conf_accuracy"] is not None]
    labels = [row["volume_bin"] for row in filtered]
    accuracies = [row["high_conf_accuracy"] for row in filtered]

    plt.figure(figsize=(9, 6))
    bars = plt.bar(labels, accuracies, color=["#264653", "#2a9d8f", "#8ab17d", "#e9c46a", "#f4a261"])
    plt.xlabel("Volume Bin")
    plt.ylabel("High-Confidence Prediction Accuracy")
    plt.ylim(0, 1.0)
    plt.title("High-Confidence Accuracy by Volume Bin (p>=0.6 or p<=0.4)")
    plt.grid(True, axis="y", alpha=0.25)
    for bar, row in zip(bars, filtered):
        x = bar.get_x() + bar.get_width() / 2
        y = row["high_conf_accuracy"]
        plt.text(x, y + 0.02, f"{y:.3f}", ha="center", va="bottom")
        plt.text(
            x,
            max(y * 0.5, 0.06),
            f"n={row['high_conf_count']}",
            ha="center",
            va="center",
            color="white",
            fontsize=9,
        )
    plt.tight_layout()
    plt.savefig(HIGH_CONFIDENCE_ACCURACY_PLOT, dpi=200)
    plt.close()
    return True


def plot_midrange_accuracy_by_volume_bin(bin_summary):
    if plt is None:
        return False

    filtered = [row for row in bin_summary if row["count"] > 0]
    labels = [row["volume_bin"] for row in filtered]
    accuracies = [row["accuracy"] for row in filtered]

    plt.figure(figsize=(9, 6))
    bars = plt.bar(labels, accuracies, color=["#6c757d", "#4d908e", "#277da1", "#577590", "#1d3557"])
    plt.xlabel("Volume Bin")
    plt.ylabel("Prediction Accuracy")
    plt.ylim(0, 1.0)
    plt.title("Accuracy by Volume Bin Excluding Extreme Probabilities (0.05 < p < 0.95)")
    plt.grid(True, axis="y", alpha=0.25)
    for bar, row in zip(bars, filtered):
        x = bar.get_x() + bar.get_width() / 2
        y = row["accuracy"]
        plt.text(x, y + 0.02, f"{y:.3f}", ha="center", va="bottom")
        plt.text(x, max(y * 0.5, 0.06), f"n={row['count']}", ha="center", va="center", color="white", fontsize=9)
    plt.tight_layout()
    plt.savefig(MIDRANGE_ACCURACY_PLOT, dpi=200)
    plt.close()
    return True


def summarize(rows, plots_created, bin_summary, extra_plot_status):
    volumes = [row["volume"] for row in rows]
    variances = [row["variance"] for row in rows]
    paired = list(zip(volumes, variances))

    volume_mean = mean(volumes)
    variance_mean = mean(variances)

    cov = sum((v - volume_mean) * (var - variance_mean) for v, var in paired) / len(paired)
    volume_std = (sum((v - volume_mean) ** 2 for v in volumes) / len(volumes)) ** 0.5
    variance_std = (sum((var - variance_mean) ** 2 for var in variances) / len(variances)) ** 0.5
    corr = cov / (volume_std * variance_std) if volume_std and variance_std else 0.0

    print(f"Rows used: {len(rows)}")
    print(f"Average variance: {variance_mean:.6f}")
    print(f"Average volume: {volume_mean:.2f}")
    print(f"Correlation(volume, variance): {corr:.6f}")
    print(f"Saved CSV: {OUTPUT_CSV}")
    print("Volume-bin summary:")
    for row in bin_summary:
        print(
            f"  {row['volume_bin']}: n={row['count']}, avg_variance={row['avg_variance']:.6f}, accuracy={row['accuracy']:.6f}, high_conf_n={row['high_conf_count']}, high_conf_accuracy={row['high_conf_accuracy']:.6f}"
        )
    if plots_created:
        print(f"Saved plot: {SCATTER_PLOT}")
        print(f"Saved plot: {TIME_PLOT}")
    else:
        print("Skipped plots: matplotlib is not installed in this Python environment.")
        print("Install it with: python3 -m pip install matplotlib")
    if extra_plot_status["bar"]:
        print(f"Saved plot: {BINNED_PLOT}")
    if extra_plot_status["box"]:
        print(f"Saved plot: {BOXPLOT}")
    if extra_plot_status["log_binned"]:
        print(f"Saved plot: {LOG_BINNED_SCATTER}")
    if extra_plot_status["accuracy"]:
        print(f"Saved plot: {ACCURACY_PLOT}")
    if extra_plot_status["high_conf_accuracy"]:
        print(f"Saved plot: {HIGH_CONFIDENCE_ACCURACY_PLOT}")
    if extra_plot_status["midrange_accuracy"]:
        print(f"Saved plot: {MIDRANGE_ACCURACY_PLOT}")


def main():
    rows = load_rows()
    bin_summary = summarize_volume_bins(rows)
    midrange_bin_summary, midrange_rows = summarize_volume_bins_for_subset(rows)
    log_bin_summary = build_log_volume_bins(rows)
    write_output_csv(rows)
    scatter_created = plot_variance_vs_volume(rows)
    time_created = plot_variance_over_time(rows)
    extra_plot_status = {
        "bar": plot_variance_by_volume_bin(bin_summary),
        "box": plot_variance_boxplot_by_volume_bin(bin_summary),
        "log_binned": plot_variance_vs_log_volume_binned(log_bin_summary),
        "accuracy": plot_accuracy_by_volume_bin(bin_summary),
        "high_conf_accuracy": plot_high_confidence_accuracy_by_volume_bin(bin_summary),
        "midrange_accuracy": plot_midrange_accuracy_by_volume_bin(midrange_bin_summary),
    }
    summarize(rows, scatter_created and time_created, bin_summary, extra_plot_status)
    print("Midrange-probability subset summary (0.05 < p < 0.95):")
    print(f"  Rows used: {len(midrange_rows)}")
    for row in midrange_bin_summary:
        print(f"  {row['volume_bin']}: n={row['count']}, accuracy={row['accuracy']:.6f}")


if __name__ == "__main__":
    main()
