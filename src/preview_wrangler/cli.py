"""Command-line interface for Preview Wrangler."""

import logging
import sys
from pathlib import Path

import click
from dotenv import load_dotenv

from .cache import CacheManager
from .csv_downloader import CSVDownloader
from .csv_parser_fast import FastCSVParser
from .file_downloader import FileDownloader
from .inventory import InventoryManager
from .s3_client import S3Client
from .rotation_corrector_v3 import correct_rotations_v3

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

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
@click.option("--clear-cache", is_flag=True, help="Clear all cached files before starting")
@click.pass_context
def download(ctx, output_dir, clear_cache):
    """Download preview files from S3 inventory."""
    cache_manager = ctx.obj["cache_manager"]
    s3_client = ctx.obj["s3_client"]

    if clear_cache:
        click.confirm("Clear all cached files?", abort=True)
        cache_manager.clear_cache()

    try:
        # Step 1: Get latest inventory manifest
        click.echo("Finding latest inventory...")
        inventory_manager = InventoryManager(s3_client)
        manifest = inventory_manager.get_latest_manifest()
        click.echo(f"Found inventory from {manifest.creation_date}")

        # Step 2: Download CSV files
        click.echo(f"\nDownloading {len(manifest.files)} CSV files...")
        csv_downloader = CSVDownloader(s3_client, cache_manager)
        csv_paths = csv_downloader.download_csv_files(manifest)

        # Step 3: Parse CSV files
        click.echo("\nParsing CSV files for preview directories...")
        parser = FastCSVParser()
        preview_dirs = parser.parse_csv_files(csv_paths)

        if not preview_dirs:
            click.echo("No qualifying preview directories found.")
            return

        click.echo(f"Found {len(preview_dirs)} preview directories with ML upload files")

        # Step 4: Download preview files
        click.echo(f"\nDownloading preview files to {output_dir}...")
        file_downloader = FileDownloader(s3_client, cache_manager, output_dir)
        file_downloader.download_preview_files(preview_dirs)

        click.echo("\nDownload complete!")

    except Exception as e:
        logger.exception("Error during download")
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
