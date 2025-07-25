# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Preview Wrangler is a Python application that processes S3 inventory data to extract and download preview files from qualifying projects. The tool processes inventory data from the base bucket `prod.ml-meta-upload.getnarrativeapp.com`.

## Key Commands

```bash
# Install dependencies with uv
uv sync

# Run the application
uv run python src/main.py download

# Run with custom project concurrency (default: 4)
uv run python src/main.py download --max-projects 8

# Correct image rotations (after download)
uv run python src/main.py correct-rotations

# Force re-process all images (overwrite existing)
uv run python src/main.py correct-rotations --overwrite

# Run tests
uv run pytest

# Run linter and formatter
uv run ruff check .
uv run ruff format .
```

## Rotation Correction

The project includes an image rotation correction feature that uses rotation data from project ML upload files:

- **Data Source**: Uses rotation metadata from `<project_uuid>.v3.gz` files
- **Accuracy**: 100% accurate rotation detection (no ML model inference needed)
- **Processing**: Applies corrections based on rotation values: `CW90`, `CW180`, `CW270`, or `None`
- **Performance**: Fast processing, limited only by image I/O

## Performance & Parallelization

### Download Parallelization
- **Project Level**: Up to 4 projects processed concurrently (configurable with `--max-projects`)
- **File Level**: Within each project, up to 20 JPEG files downloaded concurrently
- **CSV Caching**: CSV inventory files are cached locally for resumability

## Architecture

### S3 Buckets
- **Base bucket**: `prod.ml-meta-upload.getnarrativeapp.com` - Contains the actual data files
- **Inventory bucket**: `prod.ml-meta-upload.getnarrativeapp.com-inventory` - Contains inventory manifests

### Processing Pipeline
1. **Inventory Discovery**: Find latest inventory in `s3://prod.ml-meta-upload.getnarrativeapp.com-inventory/prod.ml-meta-upload.getnarrativeapp.com/Inventory/`
2. **CSV Download**: Download gzipped CSV files listed in manifest.json to local cache
3. **Preview Identification**: Find `<user_uuid>/<project_uuid>/preview.v1` directories with matching `<user_uuid>/<project_uuid>/<project_uuid>.v3.gz` files
4. **File Download**: Download 20 JPEGs per project into `<project_uuid>/` directories along with ML upload files (supports parallel project processing)

### Key Patterns
- Preview directories: `<user_uuid>/<project_uuid>/preview.v1`
- ML upload files: `<user_uuid>/<project_uuid>/<project_uuid>.v3.gz`
- JPEG files are stored within preview directories
- All downloads are cached locally for resumability
