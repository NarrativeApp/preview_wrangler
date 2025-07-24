# Preview Wrangler

A Python application that processes S3 inventory data to extract and download preview files from qualifying projects, with automatic image rotation correction.

This project will produce a directory containing 20 jpeg preview files from each qualifying project. I will describe how to process the information we have in S3 to produce these

1. Consult the latest S3 inventory data for base bucket prod.ml-meta-upload.getnarrativeapp.com in the location s3://prod.ml-meta-upload.getnarrativeapp.com-inventory/prod.ml-meta-upload.getnarrativeapp.com/Inventory/. This s3 prefix has a manifest directory per inventory, for example as of the writing of this document the most recent inventory is stored at s3://prod.ml-meta-upload.getnarrativeapp.com-inventory/prod.ml-meta-upload.getnarrativeapp.com/Inventory/2025-07-21T01-00Z/. Inside this directory is a manifest file called manifest.json with contents similar to the following example:

```json
{
  "sourceBucket" : "prod.ml-meta-upload.getnarrativeapp.com",
  "destinationBucket" : "arn:aws:s3:::prod.ml-meta-upload.getnarrativeapp.com-inventory",
  "version" : "2016-11-30",
  "creationTimestamp" : "1753059600000",
  "fileFormat" : "CSV",
  "fileSchema" : "Bucket, Key, Size, LastModifiedDate",
  "files" : [ {
    "key" : "prod.ml-meta-upload.getnarrativeapp.com/Inventory/data/eb07a5ec-3b32-469f-87ff-6940745b81f0.csv.gz",
    "size" : 90537424,
    "MD5checksum" : "25c5c4ebeb0757a91e867ee9b1d840ff"
  }, {
    "key" : "prod.ml-meta-upload.getnarrativeapp.com/Inventory/data/a9bcf95b-c452-445b-8df5-3d79dd316a1f.csv.gz",
    "size" : 4584323,
    "MD5checksum" : "91e52d8babfba0ce3ab2931eb5442a19"
  }, {
    "key" : "prod.ml-meta-upload.getnarrativeapp.com/Inventory/data/02672f60-13ef-4351-9b35-9cc3e4c34b7f.csv.gz",
    "size" : 90097520,
    "MD5checksum" : "b0ba0a2c3ef7b8b3ed39e44f213b93c0"
  }, {
    "key" : "prod.ml-meta-upload.getnarrativeapp.com/Inventory/data/000abab4-e840-421f-aea9-1b6bb71a942b.csv.gz",
    "size" : 29978247,
    "MD5checksum" : "4d0cfd35ecbb576c83356e3e517c10d4"
  }, {
    "key" : "prod.ml-meta-upload.getnarrativeapp.com/Inventory/data/2bbe6282-b8c6-42cf-93db-b4ca1204f367.csv.gz",
    "size" : 90508378,
    "MD5checksum" : "4525738660b7c9dfb68f636891d1f37c"
  }, {
    "key" : "prod.ml-meta-upload.getnarrativeapp.com/Inventory/data/844ee0a5-7965-4b95-9d59-a0e8f5a73614.csv.gz",
    "size" : 21196182,
    "MD5checksum" : "052f8b3a1b8b71fc1cd43a68786b3512"
  }, {
    "key" : "prod.ml-meta-upload.getnarrativeapp.com/Inventory/data/c88bec9c-e524-4e95-8edf-521fc4808184.csv.gz",
    "size" : 90520972,
    "MD5checksum" : "e7d44d7d65e9e8f28464eb9d74bb3d3b"
  }, {
    "key" : "prod.ml-meta-upload.getnarrativeapp.com/Inventory/data/515fb68d-b8cc-43d2-9e24-e0b49df94a48.csv.gz",
    "size" : 28919938,
    "MD5checksum" : "b6f8865623816070ba20086b14d1dfb7"
  }, {
    "key" : "prod.ml-meta-upload.getnarrativeapp.com/Inventory/data/0e263cfb-4c2c-4be5-b7e6-dfdf9d77944e.csv.gz",
    "size" : 33277546,
    "MD5checksum" : "b6f649ab0960f98995c4d695389ad0d5"
  } ]
}
```

download all these zipped CSVs to a local named temporary directory so that they are cached if we have to re-run the script.

2. Process each of these CSV files and find all preview directories. These will be of form <user_uuid>/<project_uuid>/preview.v1 and for each of these unique directories check if there exists an ml upload file of form <user_uuid>/<project_uuid>/<project_uuid>.v3.gz. If so, then store the preview directory for the next phase of processing.

3. For each qualifying preview directory, download 20 jpegs into a local directory named <project_uuid> along with the ml upload file.

4. For each of the phases, please cache any successfully downloaded data locally so we can re-run the script if it fails. This should be written in python

5. Do not worry about any other optimisations for now. I'll see how it runs first.

6. Use uv and tell me how to run the app.

That's it!

## Features

- **S3 Inventory Processing**: Automatically finds and processes the latest S3 inventory data
- **Smart Preview Detection**: Identifies preview directories with corresponding ML upload files
- **Concurrent Downloads**: Downloads files in parallel for improved performance
- **Image Rotation Correction**: Uses rotation metadata from project ML upload files for 100% accurate corrections
  - Reads rotation data directly from `<project_uuid>.v3.gz` files
  - Corrects 90°, 180°, and 270° rotations based on actual metadata
  - No ML model inference needed - uses definitive rotation values
  - Achieves 100% accuracy with significantly improved performance
- **Resumable Operations**: All operations are cached and can be resumed if interrupted
- **Progress Tracking**: Real-time progress bars for all operations

## Installation

1. Install uv if you haven't already:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Install dependencies:
```bash
uv pip install -r requirements.txt
```

3. For development, install dev dependencies:
```bash
uv pip install -r requirements-dev.txt
```

## Usage

### Basic Usage

Run the main download command:
```bash
uv run python src/main.py download
```

Or use the CLI directly:
```bash
uv run python -m preview_wrangler.cli download
```

### Command Options

- `--output-dir, -o`: Specify output directory (default: ./output)
- `--clear-cache`: Clear all cached files before starting
- `--debug`: Enable debug logging

Example:
```bash
uv run python src/main.py download -o /path/to/output --debug
```

### Image Rotation Correction

After downloading images, you can automatically detect and correct image rotations:

```bash
# Correct rotations in the default output directory
uv run python src/main.py correct-rotations

# Correct rotations in a specific directory
uv run python src/main.py correct-rotations --input-dir /path/to/images

# Force re-process all images (ignore cache)
uv run python src/main.py correct-rotations --overwrite
```

The rotation correction feature:
- Uses rotation metadata from project v3.gz files for 100% accuracy
- Corrects images rotated by 90°, 180°, or 270° based on definitive data
- No ML model inference required - reads actual rotation values
- Caches results to avoid reprocessing
- Applies corrections in-place to the downloaded files

### Additional Commands

View cache information:
```bash
uv run python src/main.py cache-info
```

Clear cache:
```bash
uv run python src/main.py clear-cache
```

### AWS Configuration

Make sure your AWS credentials are configured:
```bash
aws configure
```

Or set environment variables:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

## Development

### Running Tests
```bash
uv run pytest
```

### Running Linters
```bash
uv run ruff check .
uv run ruff format .
```

### Pre-commit Hooks
Install pre-commit hooks:
```bash
uv run pre-commit install
```
