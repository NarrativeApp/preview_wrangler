# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Preview Wrangler is a Python application that downloads preview files from qualifying projects using marker files created by the ml-upload-lambda. The tool processes data from the base bucket `prod.ml-meta-upload.getnarrativeapp.com`.

## Key Commands

```bash
# Install dependencies with uv
uv sync

# Run the application using marker files
uv run python src/main.py download

# Run with custom project concurrency (default: 4)
uv run python src/main.py download --max-projects 8

# Look back more hours for marker files (default: 24)
uv run python src/main.py download --hours-back 48

# Limit images per project (default: 20)
uv run python src/main.py download --max-images 5

# Download all images per project (no limit)
uv run python src/main.py download --max-images 0

# Correct image rotations (after download)
uv run python src/main.py correct-rotations

# Force re-process all images (overwrite existing)
uv run python src/main.py correct-rotations --overwrite

# Sort/rename images by capture time (after download)
uv run python src/main.py capture-time-sort

# Sort with custom input directory
uv run python src/main.py capture-time-sort --input-dir my_projects

# Sort to output directory (copy files)
uv run python src/main.py capture-time-sort --output-dir sorted_images

# Force overwrite existing renamed images
uv run python src/main.py capture-time-sort --overwrite

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

## Capture Time Sorting

The project includes a capture time sorting feature that organizes images by renaming them with structured filenames:

- **Data Source**: Uses capture time, camera model, and serial number from `<project_uuid>.v3.gz` files
- **Filename Pattern**: `<model>_<camera_serial>_<capture_time>_<image_uuid>.jpg`
- **Example**: `EOS_R6_182027002738_20250712_023256_9e8161ff-abb8-4332-bb8d-096ef9d37c68.jpg`
- **Collision Avoidance**: Uses image UUID to ensure filename uniqueness
- **Processing**: Sanitizes camera model names and formats timestamps for filesystem compatibility

## Performance & Parallelization

### Download Parallelization
- **Project Level**: Up to 4 projects processed concurrently (configurable with `--max-projects`)
- **File Level**: Within each project, up to 20 JPEG files downloaded concurrently
- **Marker Scanning**: Efficient datetime-based path structure reduces S3 listing overhead

## Architecture

### S3 Buckets
- **Base bucket**: `prod.ml-meta-upload.getnarrativeapp.com` - Contains the actual data files and marker files

### Marker-Based Processing Pipeline
1. **Marker Discovery**: Scan for marker files in datetime-based paths:
   - Preview markers: `preview.v1/YYYY/MM/DD/HH/{user_id}/{project_id}`
   - V3 markers: `v3/YYYY/MM/DD/HH/{user_id}/{project_id}`
2. **Project Identification**: Find projects that have both preview.v1 and v3.gz marker files
3. **File Download**: Download up to 20 JPEGs per project into `<project_uuid>/` directories along with ML upload files (supports parallel project processing)

### Key Patterns
- Marker files are empty files created by ml-upload-lambda when files are uploaded
- Preview directories: `<user_uuid>/<project_uuid>/preview.v1`
- ML upload files: `<user_uuid>/<project_uuid>/<project_uuid>.v3.gz`
- JPEG files are stored within preview directories
- All downloads are cached locally for resumability
