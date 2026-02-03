# Copy-Missing

A focused tool to find and copy missing documents between Azure AI Search indexes.

## Features

- **Scan mode**: Parallel partition scanning using timestamp strategy to find docs missing from destination
- **Copy mode**: Serial document copying with retry and resume support
- **Persistence**: JSON-based storage of missing doc IDs for later copying

## Installation

```bash
pip install -r requirements.txt
```

## Running

From the `copy-missing` directory:

```powershell
# Option 1: Reference the src module directly
python -m src.copy_missing <command>

# Option 2: Set PYTHONPATH (PowerShell)
$env:PYTHONPATH = "src"; python -m copy_missing <command>

# Option 3: Set PYTHONPATH (bash)
PYTHONPATH=src python -m copy_missing <command>
```

## Configuration

Set these environment variables (or create a `.env` file):

| Variable | Required | Description |
|----------|----------|-------------|
| `AZURE_SEARCH_SOURCE_ENDPOINT` | Yes | Source Azure Search service URL |
| `AZURE_SEARCH_SOURCE_KEY` | No | Source admin key (uses DefaultAzureCredential if not set) |
| `AZURE_SEARCH_DEST_ENDPOINT` | Yes | Destination Azure Search service URL |
| `AZURE_SEARCH_DEST_KEY` | No | Destination admin key (uses DefaultAzureCredential if not set) |
| `TIMESTAMP_FIELD` | Yes | Field name containing document timestamps |
| `KEY_FIELD` | No | Key field name (auto-detected from index if not set) |
| `INDEX_NAME` | No | Default index name (can override with `--index`) |
| `DESIRED_PARTITIONS` | No | Number of partitions for parallel scan (default: 8) |
| `STATE_DIR` | No | Directory for state files (default: `state`) |

## Usage

### 1. Scan for missing documents

```bash
python -m src.copy_missing scan --index my-index --partitions 8
```

This will:
- Divide the timestamp range into 8 partitions
- Scan each partition in parallel
- Check which doc IDs from source don't exist in destination
- Save results to `state/my-index_missing.json`

### 2. Copy missing documents

```bash
python -m src.copy_missing copy --index my-index
```

This will:
- Load the scan results from JSON
- Copy each missing document one at a time (serial for reliability)
- Save progress after each document (supports resume on interruption)
- Retry transient failures with exponential backoff

### Resume after interruption

If copy is interrupted, just run the same command again:

```bash
python -m src.copy_missing copy --index my-index
```

It will skip already-copied documents and resume from where it left off.

To start fresh instead of resuming:

```bash
python -m src.copy_missing copy --index my-index --no-resume
```

## Output Files

| File | Description |
|------|-------------|
| `state/{index}_missing.json` | Scan results with list of missing doc IDs |
| `state/{index}_copy_progress.json` | Copy progress (removed on completion) |

## Example .env file

```env
AZURE_SEARCH_SOURCE_ENDPOINT=https://my-source.search.windows.net
AZURE_SEARCH_SOURCE_KEY=your-source-admin-key
AZURE_SEARCH_DEST_ENDPOINT=https://my-dest.search.windows.net
AZURE_SEARCH_DEST_KEY=your-dest-admin-key
TIMESTAMP_FIELD=lastModifiedDateTime
INDEX_NAME=my-index
DESIRED_PARTITIONS=8
```
