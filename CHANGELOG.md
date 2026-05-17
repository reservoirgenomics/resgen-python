## v0.15.1

- Fix `resgen manage sync-datasets` ignoring `~/.resgen/license.jwt`: `_resolve_license` now checks `~/.resgen/license.jwt` first (the user's authoritative license), before the stale project-cached copy written by `start`
- Refactor license resolution into a shared `_resolve_license(directory)` helper; remove unused `get_license_text`
- Fix `resgen manage stop` not stopping the container: `start` and `stop` now both use an explicit `--project-name rgc-<hash>` so Docker Compose can match the running project regardless of the working directory

## v0.15.0

- Add support for [finch](https://github.com/runfinch/finch) as an alternative container runtime in `resgen manage`. The runtime is auto-detected from PATH (docker preferred for backwards compatibility) or can be set explicitly via the `RESGEN_CONTAINER_RUNTIME` environment variable

## v0.14.0

- Save credentials to `<directory>/.resgen/credentials` after the first login prompt for `resgen manage view` and `resgen manage pileup`, so subsequent runs in the same directory don't re-prompt
- `resgen manage pileup` now accepts multiple CSV files; each is opened as a separate view arranged in a grid whose column-to-row ratio approximates the golden ratio
- Each pileup track displays its source filename as a label in the bottom-left corner in dark grey

## v0.13.1

- Add chromsizes and sequence tracks to `resgen manage pileup` view: generates `.fai` index if absent, registers it as `chromsizes-tsv`, and adds `horizontal-chromosome-labels` and `horizontal-sequence` tracks above the pileup track
- Fix 500 on sequence tiles caused by `indexfile` being unset on the FASTA tileset; `indexfile` is now included in the same PATCH as the tags to avoid `TilesetSerializer` clearing tags on a second update
- Degrade gracefully when the dataset limit is reached: chromosome-labels track is skipped but sequence and pileup tracks are still shown

## v0.13.0

- Add `resgen manage pileup` command to align CSV sequences against a reference FASTA and display as a pileup
- Fix `_get_running_containers` to detect containers by name prefix (`rgc-`) rather than image ancestor, preventing port conflicts when multiple image versions are running

## v0.12.2

- Update default datatype for gff files to gene-transcripts

## v0.12.1

- Fix license reading when starting local

## v0.12.0

- Added update command to pull the latest Docker image

## v0.11.0

- Automatically create fasta indexes for .fa files
- Create horizontal-transcripts track for gff files

## v0.9.1

- View private files on S3

## v0.9.0

- View files on S3

## v0.8.0, v0.8.1

- Sequence logo plot and pileups based on csv files

## v0.7.2

- Improve error handling in find_or_create_project

## v0.7.1

- Don't assume project_name is in return from projects list

## v0.7.0

- Sync folders to projects

## v0.5.4

- Added a function for getting a download url for a dataset

## v0.5.3

- Added the --sync-full-path option to sync datasets so that a full
  url can be used as the basis for the sync behavior

## v0.5.2

- Check for projects in another user's namespace before creating
