"""Debug script to analyze inventory data."""

import csv
from pathlib import Path


def analyze_inventory():
    csv_dir = Path.home() / ".preview_wrangler" / "cache" / "csv"

    # Count different types of objects
    preview_count = 0
    ml_upload_count = 0
    total_objects = 0
    preview_projects = set()
    ml_projects = set()

    print("Analyzing CSV files...")

    for csv_file in csv_dir.glob("*.csv"):
        print(f"Processing {csv_file.name}...")

        with open(csv_file, encoding="utf-8") as f:
            reader = csv.reader(f)

            for row in reader:
                if len(row) < 2:
                    continue

                total_objects += 1
                key = row[1]  # Key is second column

                # Check for preview directories
                if "/preview.v1" in key:
                    preview_count += 1
                    # Extract project UUID
                    parts = key.split("/")
                    if len(parts) >= 2:
                        preview_projects.add(f"{parts[0]}/{parts[1]}")

                # Check for ML upload files
                if key.endswith(".v3.gz"):
                    ml_upload_count += 1
                    parts = key.split("/")
                    if len(parts) == 3:
                        ml_projects.add(f"{parts[0]}/{parts[1]}")

    print(f"\nTotal objects scanned: {total_objects:,}")
    print(f"Preview directories found: {preview_count:,}")
    print(f"Unique projects with previews: {len(preview_projects):,}")
    print(f"ML upload files found: {ml_upload_count:,}")
    print(f"Unique projects with ML uploads: {len(ml_projects):,}")

    # Find intersection
    matching_projects = preview_projects.intersection(ml_projects)
    print(f"\nProjects with BOTH preview and ML upload: {len(matching_projects):,}")

    if matching_projects:
        print("\nFirst 5 matching projects:")
        for proj in list(matching_projects)[:5]:
            print(f"  {proj}")


if __name__ == "__main__":
    analyze_inventory()
