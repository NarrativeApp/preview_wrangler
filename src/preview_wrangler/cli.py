"""Command-line interface for Preview Wrangler."""

import logging
import sys
from pathlib import Path

import click
from dotenv import load_dotenv
from tqdm import tqdm

from .cache import CacheManager
from .capture_time_sorter import capture_time_sort
from .csv_parser import PreviewDirectory
from .file_downloader import FileDownloader
from .marker_scanner import MarkerScanner
from .orphan_cleaner import OrphanCleaner
from .rotation_corrector_v3 import correct_rotations_v3
from .s3_client import S3Client

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Suppress urllib3 connection pool warnings
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx, debug):
    """Preview Wrangler - Process S3 inventory data to extract preview files."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize shared components
    ctx.ensure_object(dict)
    ctx.obj["cache_manager"] = CacheManager()

    try:
        ctx.obj["s3_client"] = S3Client()
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    default="output",
    help="Output directory for downloaded files",
)
@click.option(
    "--hours-back",
    type=int,
    default=24,
    help="Number of hours to look back for new projects",
)
@click.option(
    "--max-projects",
    type=int,
    default=4,
    help="Maximum number of projects to download concurrently (parallel processing)",
)
@click.option(
    "--max-images",
    type=int,
    help="Maximum number of images to download per project (default: all)",
)
@click.option(
    "--limit",
    type=int,
    help="Limit the total number of projects to download (0 = no limit)",
)
@click.pass_context
def download(ctx, output_dir, hours_back, max_projects, max_images, limit):
    """Download preview files using marker files."""
    cache_manager = ctx.obj["cache_manager"]
    s3_client = ctx.obj["s3_client"]

    try:
        # Step 1: Scan for projects with marker files
        click.echo(f"Scanning for projects from the last {hours_back} hours...")
        scanner = MarkerScanner(s3_client)
        projects = scanner.scan_for_projects(hours_back=hours_back)

        if not projects:
            click.echo("No qualifying projects found.")
            return

        click.echo(f"Found {len(projects)} projects with both preview.v1 and v3.gz data")

        # Apply limit if specified
        if limit and limit > 0 and limit < len(projects):
            projects = projects[:limit]
            click.echo(f"Limited to first {limit} projects")

        # Step 2: Convert to preview directory format expected by FileDownloader
        click.echo("Converting to preview directory format...")
        preview_dirs = []
        with tqdm(total=len(projects), desc="Preparing projects") as pbar:
            for user_id, project_id in projects:
                # Skip the expensive get_project_files call - we know the structure
                preview_dirs.append(
                    PreviewDirectory(
                        user_uuid=user_id,
                        project_uuid=project_id,
                        preview_path=f"{user_id}/{project_id}/preview.v1/",
                        ml_upload_path=f"{user_id}/{project_id}/{project_id}.v3.gz",
                    )
                )
                pbar.update(1)

        click.echo(f"Created {len(preview_dirs)} preview directory objects")

        # Step 3: Download preview files
        click.echo(f"\nDownloading preview files to {output_dir}...")
        file_downloader = FileDownloader(
            s3_client=s3_client,
            cache_manager=cache_manager,
            output_dir=output_dir,
            max_project_workers=max_projects,
            max_images=max_images,
        )
        file_downloader.download_preview_files(preview_dirs)

        click.echo("\nDownload complete!")

    except Exception as e:
        logger.exception("Error during marker-based download")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def clear_cache(ctx):
    """Clear all cached files."""
    cache_manager = ctx.obj["cache_manager"]
    click.confirm("Clear all cached files?", abort=True)
    cache_manager.clear_cache()
    click.echo("Cache cleared.")


@cli.command()
@click.pass_context
def cache_info(ctx):
    """Show cache information."""
    cache_manager = ctx.obj["cache_manager"]

    # Calculate cache size
    cache_size = 0
    file_count = 0

    for path in cache_manager.cache_dir.rglob("*"):
        if path.is_file():
            cache_size += path.stat().st_size
            file_count += 1

    click.echo(f"Cache directory: {cache_manager.cache_dir}")
    click.echo(f"Files cached: {file_count}")
    click.echo(f"Total size: {cache_size / (1024**3):.2f} GB")

    # Show cached CSV files
    csv_count = len(list(cache_manager.get_csv_cache_dir().glob("*.csv")))
    click.echo(f"CSV files: {csv_count}")


@cli.command()
@click.option(
    "--input-dir",
    "-i",
    type=click.Path(exists=True, path_type=Path),
    default="preview_projects",
    help="Input directory containing project directories with images and v3.gz files",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    help="Output directory for corrected images (default: in-place)",
)
@click.option("--overwrite", is_flag=True, help="Overwrite existing corrected images")
def correct_rotations_cmd(input_dir, output_dir, overwrite):
    """Correct image rotations using data from project v3.gz files."""
    try:
        click.echo(f"Processing projects in {input_dir}...")

        correct_rotations_v3(
            input_dir=str(input_dir),
            output_dir=str(output_dir) if output_dir else None,
            overwrite=overwrite,
        )

    except Exception as e:
        logger.exception("Error during rotation correction")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--input-dir",
    "-i",
    type=click.Path(exists=True, path_type=Path),
    default="output",
    help="Input directory containing project directories with images and v3.gz files",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    help="Output directory for renamed images (default: in-place)",
)
@click.option("--overwrite", is_flag=True, help="Overwrite existing renamed images")
def capture_time_sort_cmd(input_dir, output_dir, overwrite):
    """Sort/rename images by capture time using data from project v3.gz files."""
    try:
        click.echo(f"Processing projects in {input_dir}...")

        capture_time_sort(
            input_dir=str(input_dir),
            output_dir=str(output_dir) if output_dir else None,
            overwrite=overwrite,
        )

    except Exception as e:
        logger.exception("Error during capture time sorting")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--date-from",
    type=str,
    help="Start date for valid marker files (YYYY-MM-DD format, e.g., 2024-01-01)",
)
@click.option(
    "--date-to",
    type=str,
    help="End date for valid marker files (YYYY-MM-DD format, e.g., 2024-01-31). Defaults to today.",
)
@click.option(
    "--days-back",
    type=int,
    default=7,
    help="Number of days to look back for valid marker files (default: 7). Ignored if --date-from is specified.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=True,
    help="Perform a dry run without actually deleting files (default: dry-run)",
)
@click.option(
    "--report",
    type=click.Path(path_type=Path),
    help="Save a detailed report to this file",
)
@click.option(
    "--batch-size",
    type=int,
    default=1000,
    help="Number of files to delete in each batch",
)
@click.pass_context
def clean_orphans(ctx, date_from, date_to, days_back, dry_run, report, batch_size):
    """Find and delete orphaned PREVIEW files (preserves ML upload files).

    This command scans for all preview data in the bucket and identifies projects
    that don't have valid marker files created within the specified time window.
    Only preview files (under preview.v1/) are deleted - ML upload files (.v3.gz) are preserved.
    By default, it performs a dry run to show what would be deleted.
    """
    s3_client = ctx.obj["s3_client"]
    cache_manager = ctx.obj["cache_manager"]

    try:
        cleaner = OrphanCleaner(s3_client, cache_manager)

        # Parse date arguments
        start_datetime = None
        end_datetime = None

        if date_from:
            try:
                from datetime import datetime, timezone

                start_datetime = datetime.strptime(date_from, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                click.echo(
                    f"Error: Invalid date format for --date-from: {date_from}. Use YYYY-MM-DD format.",
                    err=True,
                )
                sys.exit(1)

        if date_to:
            try:
                from datetime import datetime, timezone

                # Set to end of day (23:59:59)
                end_datetime = datetime.strptime(date_to, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59, tzinfo=timezone.utc
                )
            except ValueError:
                click.echo(
                    f"Error: Invalid date format for --date-to: {date_to}. Use YYYY-MM-DD format.",
                    err=True,
                )
                sys.exit(1)

        # Display date range info
        if start_datetime and end_datetime:
            click.echo(f"Scanning for orphaned data (marker window: {date_from} to {date_to})...")
        elif start_datetime:
            click.echo(f"Scanning for orphaned data (marker window: from {date_from} to today)...")
        else:
            click.echo(f"Scanning for orphaned data (marker window: last {days_back} days)...")

        # Find orphaned data (and optionally non-orphaned for analysis)
        orphaned_files, total_size, non_orphaned_files = cleaner.find_orphaned_data(
            days_back=days_back,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            return_non_orphaned=True,
        )

        if not orphaned_files:
            click.echo("No orphaned data found. All projects have valid markers.")
            return

        # Calculate statistics
        total_projects = len(orphaned_files)
        total_files = sum(len(files) for files in orphaned_files.values())
        total_size_gb = total_size / (1024**3)

        # Collect all distinct date-hours with counts for orphaned files
        orphaned_datetime_counts = {}
        for files in orphaned_files.values():
            for _, last_modified in files:
                # Extract date and hour (first 13 characters: YYYY-MM-DDTHH)
                if len(last_modified) >= 13:
                    datetime_str = last_modified[:13]  # YYYY-MM-DDTHH
                elif len(last_modified) >= 10:
                    datetime_str = last_modified[:10]  # Just date if no time
                else:
                    datetime_str = last_modified

                if datetime_str != "unknown":
                    orphaned_datetime_counts[datetime_str] = (
                        orphaned_datetime_counts.get(datetime_str, 0) + 1
                    )

        # Collect date-hours for non-orphaned files if available
        non_orphaned_datetime_counts = {}
        if non_orphaned_files:
            for files in non_orphaned_files.values():
                for _, last_modified in files:
                    if len(last_modified) >= 13:
                        datetime_str = last_modified[:13]
                    elif len(last_modified) >= 10:
                        datetime_str = last_modified[:10]
                    else:
                        datetime_str = last_modified

                    if datetime_str != "unknown":
                        non_orphaned_datetime_counts[datetime_str] = (
                            non_orphaned_datetime_counts.get(datetime_str, 0) + 1
                        )

        click.echo(
            f"\nFound {total_projects} orphaned projects with {total_files} total files ({total_size_gb:.2f} GB)"
        )

        # Show non-orphaned stats if available
        if non_orphaned_files:
            non_orphaned_projects = len(non_orphaned_files)
            non_orphaned_file_count = sum(len(files) for files in non_orphaned_files.values())
            click.echo(
                f"Found {non_orphaned_projects} non-orphaned projects with {non_orphaned_file_count} total files"
            )

        # Show combined orphaned/non-orphaned file counts by date/hour
        if orphaned_datetime_counts or non_orphaned_datetime_counts:
            all_datetimes = sorted(
                set(orphaned_datetime_counts.keys()) | set(non_orphaned_datetime_counts.keys())
            )
            click.echo(
                f"\nFiles span {len(all_datetimes)} distinct date-hours from {all_datetimes[0]} to {all_datetimes[-1]}"
            )

            # Show all date-hours with both orphaned and non-orphaned counts
            click.echo("File counts by date and hour (orphaned / non-orphaned):")
            for datetime_str in all_datetimes:
                orphaned_count = orphaned_datetime_counts.get(datetime_str, 0)
                non_orphaned_count = non_orphaned_datetime_counts.get(datetime_str, 0)
                click.echo(f"  {datetime_str}: {orphaned_count:6d} / {non_orphaned_count:6d} files")

        # Generate report if requested
        if report:
            click.echo(f"\nGenerating report: {report}")
            cleaner.generate_report(orphaned_files, output_file=report)

        # Show sample of what would be deleted
        if dry_run:
            click.echo(f"\nDRY RUN MODE - No files will be deleted")
            click.echo(f"Total that would be deleted: {total_files} files ({total_size_gb:.2f} GB)")
            click.echo(f"Files will be deleted in batches of {batch_size}")

            # Sort projects by their most recent file date to show most recent ones first
            def get_most_recent_date(project_path):
                files = orphaned_files[project_path]
                if not files:
                    return "0000-00-00"  # Fallback for empty projects
                # Get the most recent datetime from all files in this project
                datetimes = []
                for _, last_modified in files:
                    if len(last_modified) >= 13:
                        datetimes.append(last_modified[:13])  # YYYY-MM-DDTHH
                    elif len(last_modified) >= 10:
                        datetimes.append(last_modified[:10])  # Just date
                    else:
                        datetimes.append("0000-00-00")
                return max(datetimes) if datetimes else "0000-00-00"

            projects_by_recency = sorted(
                orphaned_files.keys(), key=get_most_recent_date, reverse=True
            )

            click.echo("\nSample of orphaned projects and files (most recent 5 projects):")
            for _i, project_path in enumerate(projects_by_recency[:5]):
                files = orphaned_files[project_path]
                click.echo(f"  {project_path}: {len(files)} files")
                # Show first 30 files as examples with dates and hours
                for file_path, last_modified in files[:30]:
                    # Format the datetime for better readability
                    if len(last_modified) >= 13:
                        datetime_str = last_modified[:13]  # YYYY-MM-DDTHH
                    elif len(last_modified) >= 10:
                        datetime_str = last_modified[:10]  # Just date
                    else:
                        datetime_str = last_modified
                    click.echo(f"    - {file_path} (modified: {datetime_str})")
                if len(files) > 30:
                    click.echo(f"    ... and {len(files) - 30} more files")
                click.echo("")

            if total_projects > 5:
                click.echo(f"  ... and {total_projects - 5} more projects")

            click.echo("\nTo actually delete these files, run with --no-dry-run")
        else:
            # Confirm deletion
            click.confirm(
                f"\nWARNING: This will delete {total_files} files ({total_size_gb:.2f} GB) from {total_projects} projects. Continue?",
                abort=True,
            )

            # Delete the files
            projects_deleted, files_deleted = cleaner.delete_orphaned_data(
                orphaned_files, dry_run=False, batch_size=batch_size
            )

            click.echo(f"\nDeleted {files_deleted} files from {projects_deleted} projects")

    except Exception as e:
        logger.exception("Error during orphan cleanup")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()
