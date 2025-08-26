import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import shutil
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet

GOLD_DIR = "gold"
EDA_OUTPUT = "eda_outputs"
REPORT_FILE = "EDA_Report.pdf"
SUMMARY_DIR = "eda_summaries"

# Clean outputs every run
for folder in [EDA_OUTPUT, SUMMARY_DIR]:
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.makedirs(folder, exist_ok=True)

# PDF setup
styles = getSampleStyleSheet()
report_elements = []


def get_summary_text(file_name, df):
    """Return summary string for a dataframe"""
    summary = []
    summary.append(f"📊 Summary for {file_name}\n")
    summary.append("=" * 60 + "\n")
    summary.append("Data Types:\n")
    summary.append(str(df.dtypes) + "\n\n")
    summary.append("Descriptive Statistics:\n")
    summary.append(str(df.describe(include="all").fillna("").round(2)) + "\n")
    return "\n".join(summary)


def save_summary_to_txt(file_name, summary_text):
    """Save summary string to txt file"""
    file_path = os.path.join(SUMMARY_DIR, file_name.replace(".csv", "_summary.txt"))
    with open(file_path, "w") as f:
        f.write(summary_text)
    print(f"✅ Saved summary: {file_path}")


def run_eda():
    for file in os.listdir(GOLD_DIR):
        if not file.endswith(".csv"):
            continue

        file_path = os.path.join(GOLD_DIR, file)
        df = pd.read_csv(file_path)

        print(f"\n Running EDA for {file}")

        # --- Generate summary ---
        summary_text = get_summary_text(file, df)
        save_summary_to_txt(file, summary_text)

        # Add summary to PDF
        report_elements.append(Paragraph(f"<b>{file}</b>", styles["Heading2"]))
        report_elements.append(Spacer(1, 12))
        for line in summary_text.split("\n"):
            report_elements.append(Paragraph(line, styles["Normal"]))
        report_elements.append(Spacer(1, 12))

        chart_path = None

        # city_kpis → bar chart
        if "city_kpis" in file:
            top5 = df.nlargest(5, "total_pickups")
            plt.figure(figsize=(8, 5))
            sns.barplot(x="city", y="total_pickups", data=top5, palette="viridis")
            plt.title("Top 5 Cities by Total Pickups", fontsize=14, fontweight="bold")
            plt.xlabel("City", fontsize=12)
            plt.ylabel("Total Pickups", fontsize=12)
            plt.xticks(rotation=45)
            plt.tight_layout()
            chart_path = os.path.join(EDA_OUTPUT, "city_kpis_bar.png")
            plt.savefig(chart_path)
            plt.close()

        # daily_kpis → histogram
        elif "daily_kpis" in file.lower():
            if "avg_revenue_per_trip_usd" in df.columns:
                plt.figure(figsize=(8, 5))
                sns.histplot(df["avg_revenue_per_trip_usd"], bins=20, kde=True, color="steelblue")
                plt.title("Distribution of Avg Revenue per Trip (USD)", fontsize=14, fontweight="bold")
                plt.xlabel("Avg Revenue per Trip (USD)", fontsize=12)
                plt.ylabel("Frequency", fontsize=12)
                plt.tight_layout()
                chart_path = os.path.join(EDA_OUTPUT, "daily_kpis_hist.png")
                plt.savefig(chart_path)
                plt.close()

        # driver_stats → correlation heatmap
        elif "driver_stats" in file:
            corr = df.corr(numeric_only=True)
            if corr.shape[0] > 5:
                corr = corr.iloc[:5, :5]
            plt.figure(figsize=(6, 5))
            sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f")
            plt.title("Driver Stats Correlation Heatmap (Top 5)", fontsize=14, fontweight="bold")
            plt.tight_layout()
            chart_path = os.path.join(EDA_OUTPUT, "driver_stats_corr.png")
            plt.savefig(chart_path)
            plt.close()

        # rider_stats → barplot for top 5 riders
        elif "rider_stats" in file:
            if "rider_id" in df.columns and "trips" in df.columns:
                top5 = df.nlargest(5, "trips")
                plt.figure(figsize=(8, 5))
                sns.barplot(x="rider_id", y="trips", data=top5, palette="plasma")
                plt.title("Top 5 Riders by Trips", fontsize=14, fontweight="bold")
                plt.xlabel("Rider ID", fontsize=12)
                plt.ylabel("Number of Trips", fontsize=12)
                plt.xticks(rotation=45)
                plt.tight_layout()
                chart_path = os.path.join(EDA_OUTPUT, "rider_stats_top5.png")
                plt.savefig(chart_path)
                plt.close()

        elif "dashboard" in file:
            print("Skipping chart for dashboard.csv")

        # Add chart to PDF
        if chart_path:
            report_elements.append(Image(chart_path, width=400, height=250))
            report_elements.append(Spacer(1, 24))
            print(f"✅ Saved chart: {chart_path}")


if __name__ == "__main__":
    print("Starting EDA Analysis...")

    run_eda()

    print("Generating PDF report (summary + charts)...")
    doc = SimpleDocTemplate(REPORT_FILE, pagesize=A4)
    report_elements.insert(0, Paragraph("<b>Medallion Data Pipeline - EDA Report</b>", styles["Title"]))
    report_elements.insert(1, Spacer(1, 24))
    doc.build(report_elements)

    print(f"✅ EDA completed. Report saved as {REPORT_FILE}")
    print(f"✅ Summaries saved in '{SUMMARY_DIR}/'")
