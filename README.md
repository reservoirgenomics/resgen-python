# resgen-python

## Installation

```
pip install resgen-python
```

## Documentation

Documentation can be found at [docs-python.resgen.io](https://docs-python.resgen.io)

## CLI usage

### `resgen manage pileup`

Align sequences in one or more CSV files against a reference FASTA and open the result as a pileup view in HiGlass.

```
resgen manage pileup -ref <reference.fa> -t colname:<col> <file1.csv> [file2.csv ...]
```

**Options**

| Flag | Description |
|---|---|
| `-ref` / `--reference` | Reference FASTA file (required) |
| `-t` / `--tag` | Column selector and other tags, e.g. `-t colname:seq` or `-t colnum:3` (required) |
| `-th` / `--track-height` | Pileup track height in pixels (default: 100) |

**Multiple CSV files**

Pass any number of CSV files and each one is opened as a separate view. The views are arranged in a grid whose column-to-row ratio approximates the golden ratio (≈ 1.618):

| Files | Grid |
|---|---|
| 1 | 1 × 1 |
| 2 | 2 × 1 |
| 3 | 2 × 2 |
| 4 | 2 × 2 |
| 5 | 3 × 2 |
| 6 | 3 × 2 |

```bash
resgen manage pileup -ref ref.fa -t colname:seq sample1.csv sample2.csv sample3.csv
```

## Testing

```
pip install -r requirements-dev.txt
pytest
```
