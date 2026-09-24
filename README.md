# DriveScanner

Folder-level shared-drive inventory for cloud migration planning. Scans a selected drive or subfolder, classifies file types, records aggregate metadata and optional duplicate signals, and writes a CSV, tree, and JSON summary.

## Install

Requires Python 3 and PyYAML. In PowerShell, from the directory containing the repository files:

```powershell
python -m pip install -r .\requirements.txt
```

The scanner reads source-file metadata; duplicate detection additionally reads the beginning of source files. It does **not** move or delete source files. Choose an output directory **outside** the source tree.

## Run a selected subfolder at maximum depth

Change the example paths to your actual source and output locations:

```powershell
python .\driveMap_scanner_v3_duplicate_mode.py `
    --config ".\driveMap_scannerConfig.yaml" `
    --root "R:\_Staff\Derek" `
    --label "Derek" `
    --out "C:\DriveMap\Derek" `
    --max-depth 999999 `
    --duplicate-mode full `
    --progress-every 10 `
    --heartbeat-seconds 30
```

A subfolder passed to `--root` is the start of the inventory; the script does not scan sibling or parent folders. You may omit `--max-depth`: the current default is 999999 (effectively unlimited). `--max-depth 0` emits only the root **folder row**, although FULL aggregation for that folder may still examine files in its descendants.

### Maximum depth is not the same as FULL scan detail

`--max-depth` determines how many folder levels receive inventory rows. Separately, `scan_modes` in the YAML decides whether an individual folder uses FULL or SAMPLED **file aggregation**. To force FULL aggregation for the chosen subtree, add a distinctive fragment of that directory's path to `scan_modes.full_scan_paths` in `driveMap_scannerConfig.yaml`. Keep the other existing entries:

```yaml
scan_modes:
  full_scan_paths:
    - "R:/_Staff/Derek"
    - "Demobase"
    # Keep the remaining configured full_scan_paths here.
```

The match is case-insensitive and treats backslashes as forward slashes. FULL-path matches override SAMPLED-path matches, so the example overrides the default `_Staff` sampled rule for this specific subtree. Path rules are substring matches, not exact-path matches: choose a sufficiently specific fragment so unrelated paths will not accidentally match. No CLI option directly forces FULL routing without changing the YAML.

If neither path list matches a folder, more than `thresholds.file_count_full_mode` direct files (default 500) triggers FULL; otherwise it is SAMPLED. Even SAMPLED routing can walk descendant directories when selecting the sample.

### Duplicate-detection choices

`--duplicate-mode off` (the repository configuration default) skips file hashing. `--duplicate-mode sample` hashes a capped group of same-size/same-extension candidate files. `--duplicate-mode full` hashes every file included in that folder's aggregation. The full option is **not full-file hashing**: the default is the first 1 MiB of each checked file. Matching partial hashes are only potential duplicates, not verified identical files. In a SAMPLED folder, duplicate mode `full` still operates only on the files included in the sample. Duplicate groups are computed within each folder's aggregation, not as a global, unique-file inventory across the drive.

For maximum available detail, combine (1) effectively unlimited depth, (2) a matching `full_scan_paths` rule, and (3) `--duplicate-mode full`. On a large tree, FULL recursive aggregation repeatedly processes the same descendant files when aggregating their ancestor folders and can be expensive on network drives.

## CLI reference

| Option | Behavior |
| --- | --- |
| `--config PATH` | Required YAML configuration path. |
| `--root PATH` | Required source folder; repeat to scan multiple roots. |
| `--label NAME` | Optional label; repeat once for each root, or omit labels entirely to use folder names. |
| `--out PATH` | Required output directory. Use a distinct directory for separate scans. |
| `--max-depth N` | Folder-row traversal depth below each root; omitted defaults to 999999. |
| `--duplicate-mode off|sample|full` | Override duplicate mode for this run. |
| `--duplicate-sample-max-files N` | Candidate-file cap for sample duplicate mode. |
| `--duplicate-sample-first-bytes N` | Bytes hashed per candidate in sample duplicate mode. |
| `--progress-every N` | Print folder-loop progress every N folders. |
| `--walk-progress-every-dirs N` | Print recursive-walk progress every N directories. |
| `--file-progress-every N` | Print metadata/hash progress every N files. |
| `--heartbeat-seconds N` | Periodic progress messages during lengthy operations. |

For progress-count options, 0 disables count-based messages; `--heartbeat-seconds 0` disables the timed heartbeat. `--no-duplicate-detection` is a backward-compatible alias for disabling hashing. `--duplicate-mode` is the explicit setting to use.

## Output and restart

Under `--out`, the scanner writes:

- `folder_inventory.csv`: one row per scanned folder; direct counts, aggregate metadata, estimated/observed sizes, file classifications, duplicate signals, and empty columns for later manual review.
- `tree.txt`: a hierarchical folder listing with direct subfolder and file counts.
- `summary.json`: totals, inaccessible directories, and a sample of file errors.
- `checkpoint.json`: periodically saved progress; deleted on successful completion.

If a checkpoint exists on startup, the program asks whether to resume. Resume requires the same YAML **file contents**, roots, and labels. Checkpoints are normally written every 100 folders, so work done after the last checkpoint may need to be repeated after an interruption. CSV content is appended when resuming, but the tree and summary are generated only on successful completion; inspect outputs for completeness after a failed/interrupted run. Use a **new output directory** for a different scan to avoid mixing results or overwriting an existing inventory.

## Interpretation and known limitations

- FULL aggregation scans the current folder **and its descendants**. Thus parent rows overlap child rows; adding `folder_size_bytes` across rows will double-count data.
- In SAMPLED mode, when direct files are at or below the configured sample target (default 50), only those direct files are analyzed. Above that threshold, the scanner gathers descendant candidates for sampling but supplies the **direct-file count** to aggregation. Treat size and age measures in sampled rows as estimates or sample observations, not exact recursive folder totals.
- This is a **folder-level**, not file-level, inventory; duplicate identifiers are partial-hash summaries, not verified duplicate-file pairs.
- Inaccessible directories and unreadable files can yield incomplete results. Inspect `summary.json` before relying on the inventory.
- The YAML `hashing` and `ownership` blocks are retained for future expansion but are not read by this script version.

See `driveMap_scannerConfig.yaml` for path-routing rules, sample sizes, checkpoint frequency, file-type extensions, and duplicate-detection settings.
