"""Command-line interface for Preview Wrangler."""

import logging
import sys
from pathlib import Path

import click
from dotenv import load_dotenv
from tqdm import tqdm

from .cache import CacheManager
from .csv_parser import PreviewDirectory
from .file_downloader import FileDownloader
from .marker_scanner import MarkerScanner
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
    help="Maximum number of projects to download concurrently",
)
@click.option(
    "--limit",
    type=int,
    help="Limit the total number of projects to download",
)
@click.pass_context
def download(ctx, output_dir, hours_back, max_projects, limit):
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
        file_downloader = FileDownloader(s3_client, cache_manager, output_dir, max_projects)
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


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()
