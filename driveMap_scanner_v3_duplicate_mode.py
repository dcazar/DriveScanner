"""
driveMap_scanner.py

Scans a shared drive/folder tree to support cloud migration planning.

Core functionality preserved:
- YAML config loading
- Config hash
- Checkpoint resume/restart
- FULL vs SAMPLED folder scan routing
- Sampling: largest + newest + random
- Folder-level aggregation
- Incremental CSV writing
- tree.txt output
- summary.json output

Example:
    python driveMap_scanner.py ^
      --config config.yaml ^
      --root "Q:\\" ^
      --label Qmap ^
      --out "C:\\inventory\\Qmap" ^
      --max-depth 6
"""

import argparse
import csv
import hashlib
import heapq
import json
import os
import random
import sys
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml


# -----------------------------
# Default configuration values
# -----------------------------

DEFAULT_CONFIG: Dict[str, Any] = {
    "scan_modes": {
        "full_scan_paths": [],
        "sampled_scan_paths": [],
    },
    "thresholds": {
        # If a folder has more direct files than this, use FULL mode unless
        # overridden by sampled_scan_paths.
        "file_count_full_mode": 500,
    },
    "sampling": {
        "total_sampled_files": 50,
        "largest_n": 10,
        "newest_n": 10,
        "random_n": 30,
        # Optional. Set to an integer for reproducible sampling.
        "random_seed": None,
    },
    "performance": {
        "max_random_pool_size": 5000,
        "checkpoint_frequency": 100,
        "csv_flush_frequency": 25,
        # Duplicate detection is off by default because migration tools/IT can
        # usually handle this better and more safely at migration time.
        # Options: "off", "sample", or "full".
        "duplicate_detection_mode": "off",
        # Backward-compatible flag. If older configs set duplicate_detection: true,
        # the script treats that as full duplicate detection unless mode is set.
        "duplicate_detection": False,
        # Full mode reads this many bytes from every file being checked.
        "md5_first_bytes": 1024 * 1024,
        # Sample mode reads far fewer bytes and only checks a capped candidate set
        # from files with matching size and extension. This is a weak triage signal,
        # not authoritative deduplication.
        "duplicate_sample_first_bytes": 4096,
        "duplicate_sample_max_files": 200,
    },
    "file_groups": {
        "geospatial_extensions": [
            ".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".cpg",
            ".gdb", ".gpkg", ".geojson", ".json", ".kml", ".kmz",
            ".tif", ".tiff", ".img", ".vrt", ".las", ".laz",
            ".mxd", ".aprx", ".lyrx", ".qgz", ".qgs",
        ],
        "document_extensions": [
            ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".xlsm",
            ".ppt", ".pptx", ".txt", ".rtf", ".csv",
        ],
        "software_extensions": [
            ".py", ".r", ".R", ".sql", ".ipynb", ".js", ".ts",
            ".html", ".css", ".bat", ".ps1", ".sh", ".java",
            ".c", ".cpp", ".cs", ".vb", ".xml", ".yaml", ".yml",
        ],
    },
    "output": {
        "human_classification_columns": [
            "business_owner",
            "migration_priority",
            "target_cloud_pattern",
            "retain_archive_delete",
            "review_notes",
        ]
    },
}


CSV_FIELDNAMES = [
    "root_label",
    "path",
    "rel_path",
    "depth",
    "direct_subfolders",
    "direct_files",
    "scan_mode",
    "folder_size_bytes",
    "largest_file_bytes",
    "num_files_total",
    "newest_file_mtime_utc",
    "oldest_file_mtime_utc",
    "percent_files_older_5y",
    "percent_files_older_10y",
    "top_extensions_csv",
    "is_geospatial",
    "is_document",
    "is_software",
    "num_duplicates",
    "duplicate_ids",
]


# -----------------------------
# Basic helpers
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def progress_print(message: str) -> None:
    print(f"[{utc_now_iso()}] {message}", flush=True)


def mtime_to_utc_iso(mtime: Optional[float]) -> str:
    if mtime is None:
        return ""
    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


def normalize_path_for_match(path: str) -> str:
    return path.lower().replace("\\", "/")


def safe_path_str(path: Path) -> str:
    try:
        return str(path)
    except Exception:
        return repr(path)


def merge_config(defaults: Dict[str, Any], loaded: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merges loaded config into defaults.

    This improves reliability by allowing config.yaml to specify only the values
    that differ from defaults.
    """
    result = dict(defaults)

    for key, value in (loaded or {}).items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = merge_config(result[key], value)
        else:
            result[key] = value

    return result


def load_config(config_path: Path) -> Dict[str, Any]:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    return merge_config(DEFAULT_CONFIG, raw)


def compute_config_hash(path: Path) -> str:
    raw_bytes = path.read_bytes()
    return hashlib.sha256(raw_bytes).hexdigest()


def load_checkpoint(path: Path) -> Optional[Dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8")
        return json.loads(text)
    except Exception:
        return None


def write_json_atomic(path: Path, data: Dict[str, Any]) -> None:
    """
    Writes JSON using a temporary file and atomic replace.

    This is safer on long scans because a crash or network interruption is less
    likely to leave a half-written checkpoint or summary.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def write_checkpoint(
    path: Path,
    completed_set: set,
    queue_list: Sequence[Path],
    cfg_hash: str,
    roots: Sequence[Path],
    labels: Sequence[str],
) -> None:
    data = {
        "completed_paths": sorted(completed_set),
        "remaining_queue": [str(p) for p in queue_list],
        "config_hash": cfg_hash,
        "roots": [str(r) for r in roots],
        "root_labels": list(labels),
        "timestamp": utc_now_iso(),
    }
    write_json_atomic(path, data)


# -----------------------------
# Filesystem helpers
# -----------------------------

class ScanStats:
    """
    Tracks scan-level warnings/errors without stopping the whole run.
    """

    def __init__(self) -> None:
        self.pruned_directories: Dict[str, str] = {}
        self.file_errors: Dict[str, str] = {}
        self.rows_written = 0

    def add_pruned_dir(self, path: Path, reason: str) -> None:
        self.pruned_directories[safe_path_str(path)] = reason

    def add_file_error(self, path: Path, reason: str) -> None:
        self.file_errors[safe_path_str(path)] = reason


def safe_scandir(folder: Path, stats: ScanStats) -> Tuple[List[Path], List[Path]]:
    """
    Returns direct subfolders and direct files.

    os.scandir is materially faster than repeated Path operations because each
    DirEntry often carries file-type/stat information from the directory read.
    """
    subdirs: List[Path] = []
    files: List[Path] = []

    try:
        with os.scandir(folder) as entries:
            for entry in entries:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        subdirs.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        files.append(Path(entry.path))
                except OSError as exc:
                    stats.add_file_error(Path(entry.path), f"entry read error: {exc}")
    except OSError as exc:
        stats.add_pruned_dir(folder, f"directory read error: {exc}")

    subdirs.sort(key=lambda p: p.name.lower())
    files.sort(key=lambda p: p.name.lower())
    return subdirs, files


def safe_stat(path: Path, stats: ScanStats) -> Optional[os.stat_result]:
    try:
        return path.stat()
    except OSError as exc:
        stats.add_file_error(path, f"stat error: {exc}")
        return None


def collect_recursive_files(
    folder: Path,
    stats: ScanStats,
    progress_label: str = "",
    progress_every_dirs: int = 500,
    heartbeat_seconds: int = 30,
) -> List[Path]:
    """
    Recursively collects files under folder.

    This uses an explicit os.scandir stack instead of os.walk so progress can
    still print when one directory contains a very large number of entries.
    """
    all_files: List[Path] = []
    dirs_seen = 0
    entries_seen = 0
    started = time.time()
    last_print = started
    stack: List[Path] = [folder]

    if progress_label:
        progress_print(f"walking {progress_label}; start={folder}")

    while stack:
        current_dir = stack.pop()
        dirs_seen += 1
        subdirs_to_add: List[Path] = []

        try:
            with os.scandir(current_dir) as entries:
                for entry in entries:
                    entries_seen += 1
                    now = time.time()
                    should_print_by_count = progress_every_dirs > 0 and dirs_seen % progress_every_dirs == 0
                    should_print_by_time = heartbeat_seconds > 0 and (now - last_print) >= heartbeat_seconds

                    if progress_label and (should_print_by_count or should_print_by_time):
                        progress_print(
                            f"walking {progress_label}; dirs_seen={dirs_seen:,}; entries_seen={entries_seen:,}; "
                            f"files_found={len(all_files):,}; stack={len(stack):,}; current={current_dir}"
                        )
                        last_print = now

                    try:
                        if entry.is_dir(follow_symlinks=False):
                            candidate = Path(entry.path)
                            if not candidate.is_symlink():
                                subdirs_to_add.append(candidate)
                        elif entry.is_file(follow_symlinks=False):
                            all_files.append(Path(entry.path))
                    except OSError as exc:
                        stats.add_file_error(Path(entry.path), f"entry read error: {exc}")
        except OSError as exc:
            stats.add_pruned_dir(current_dir, f"directory read error: {exc}")
            if progress_label:
                progress_print(f"walk warning in {progress_label}; path={current_dir}; error={exc}")

        # Reverse sorted order keeps traversal deterministic enough without the
        # memory cost of building a full recursive list up front.
        try:
            subdirs_to_add.sort(key=lambda p: p.name.lower(), reverse=True)
        except Exception:
            pass
        stack.extend(subdirs_to_add)

        now = time.time()
        should_print_by_count = progress_every_dirs > 0 and dirs_seen % progress_every_dirs == 0
        should_print_by_time = heartbeat_seconds > 0 and (now - last_print) >= heartbeat_seconds
        if progress_label and (should_print_by_count or should_print_by_time):
            progress_print(
                f"walking {progress_label}; dirs_seen={dirs_seen:,}; entries_seen={entries_seen:,}; "
                f"files_found={len(all_files):,}; stack={len(stack):,}; current={current_dir}"
            )
            last_print = now

    if progress_label:
        elapsed = time.time() - started
        progress_print(
            f"finished walking {progress_label}; dirs_seen={dirs_seen:,}; entries_seen={entries_seen:,}; "
            f"files_found={len(all_files):,}; elapsed_seconds={elapsed:,.1f}"
        )

    return all_files


def md5_first_bytes(path: Path, byte_count: int, stats: ScanStats) -> Optional[str]:
    """
    Computes an MD5 hash of the first N bytes only.

    This is not a cryptographic identity for the whole file. It is a fast
    duplicate signal for migration triage.
    """
    try:
        h = hashlib.md5()
        with path.open("rb") as f:
            h.update(f.read(byte_count))
        return h.hexdigest()
    except OSError as exc:
        stats.add_file_error(path, f"hash read error: {exc}")
        return None


# -----------------------------
# Scan mode and sampling
# -----------------------------

def choose_scan_mode(path_str: str, direct_file_count: int, config_data: Dict[str, Any]) -> str:
    lower_path = normalize_path_for_match(path_str)

    for frag in config_data["scan_modes"]["full_scan_paths"]:
        if normalize_path_for_match(str(frag)) in lower_path:
            return "FULL"

    for frag in config_data["scan_modes"]["sampled_scan_paths"]:
        if normalize_path_for_match(str(frag)) in lower_path:
            return "SAMPLED"

    if direct_file_count > config_data["thresholds"]["file_count_full_mode"]:
        return "FULL"

    return "SAMPLED"


def sample_files(
    all_files: Sequence[Path],
    cfg: Dict[str, Any],
    stats: ScanStats,
) -> List[Path]:
    """
    Selects:
    - N largest files
    - N newest files
    - N random files from the remaining pool

    Faster than sorting the full list twice because heapq.nlargest only keeps
    the top N needed for largest/newest.
    """
    total_needed = int(cfg["sampling"]["total_sampled_files"])
    largest_n = int(cfg["sampling"]["largest_n"])
    newest_n = int(cfg["sampling"]["newest_n"])
    random_n = int(cfg["sampling"]["random_n"])
    max_random_pool_size = int(cfg["performance"]["max_random_pool_size"])

    if len(all_files) <= total_needed:
        return list(all_files)

    file_meta: List[Tuple[Path, int, float]] = []
    for f in all_files:
        st = safe_stat(f, stats)
        if st is not None:
            file_meta.append((f, st.st_size, st.st_mtime))

    if len(file_meta) <= total_needed:
        return [x[0] for x in file_meta]

    largest = heapq.nlargest(largest_n, file_meta, key=lambda x: x[1])
    newest = heapq.nlargest(newest_n, file_meta, key=lambda x: x[2])

    selected_paths: List[Path] = []
    used = set()

    for meta_group in (largest, newest):
        for path, _, _ in meta_group:
            path_key = str(path)
            if path_key not in used:
                selected_paths.append(path)
                used.add(path_key)

    remainder = [path for path, _, _ in file_meta if str(path) not in used]
    random_pool = remainder[:max_random_pool_size]

    seed = cfg["sampling"].get("random_seed")
    rng = random.Random(seed) if seed is not None else random

    random_chunk = rng.sample(random_pool, min(random_n, len(random_pool)))
    for path in random_chunk:
        path_key = str(path)
        if path_key not in used:
            selected_paths.append(path)
            used.add(path_key)

    return selected_paths[:total_needed]


# -----------------------------
# Aggregation
# -----------------------------

def aggregate_files(
    sampled_files: Sequence[Path],
    total_count: int,
    cfg: Dict[str, Any],
    stats: ScanStats,
    progress_label: str = "",
    progress_every_files: int = 500,
    heartbeat_seconds: int = 30,
) -> Dict[str, Any]:
    if not sampled_files:
        return {
            "folder_size_bytes": 0,
            "largest_file_bytes": 0,
            "num_files_total": total_count,
            "newest_file_mtime_utc": "",
            "oldest_file_mtime_utc": "",
            "percent_files_older_5y": "",
            "percent_files_older_10y": "",
            "top_extensions_csv": "",
            "is_geospatial": False,
            "is_document": False,
            "is_software": False,
            "num_duplicates": 0,
            "duplicate_ids": "",
        }

    now = time.time()
    five_years_seconds = 5 * 365.25 * 24 * 60 * 60
    ten_years_seconds = 10 * 365.25 * 24 * 60 * 60

    extension_counter: Counter = Counter()
    sizes: List[int] = []
    mtimes: List[float] = []
    older_5y = 0
    older_10y = 0

    geospatial_exts = {x.lower() for x in cfg["file_groups"]["geospatial_extensions"]}
    document_exts = {x.lower() for x in cfg["file_groups"]["document_extensions"]}
    software_exts = {x.lower() for x in cfg["file_groups"]["software_extensions"]}

    is_geospatial = False
    is_document = False
    is_software = False

    valid_files: List[Path] = []
    meta_started = time.time()
    last_meta_print = meta_started

    if progress_label:
        progress_print(f"aggregating {progress_label}; files_to_stat={len(sampled_files):,}; total_count={total_count:,}")

    for file_index, f in enumerate(sampled_files, start=1):
        now_for_progress = time.time()
        should_print_by_count = progress_every_files > 0 and file_index % progress_every_files == 0
        should_print_by_time = heartbeat_seconds > 0 and (now_for_progress - last_meta_print) >= heartbeat_seconds
        if progress_label and (should_print_by_count or should_print_by_time):
            progress_print(
                f"aggregating {progress_label}; stat_checked={file_index:,}/{len(sampled_files):,}; "
                f"valid_files={len(valid_files):,}; errors={len(stats.pruned_directories) + len(stats.file_errors):,}; current={f}"
            )
            last_meta_print = now_for_progress

        st = safe_stat(f, stats)
        if st is None:
            continue

        valid_files.append(f)
        sizes.append(st.st_size)
        mtimes.append(st.st_mtime)

        ext = f.suffix.lower()
        if ext:
            extension_counter[ext] += 1
        else:
            extension_counter["[no extension]"] += 1

        if ext in geospatial_exts:
            is_geospatial = True
        if ext in document_exts:
            is_document = True
        if ext in software_exts:
            is_software = True

        age_seconds = now - st.st_mtime
        if age_seconds > five_years_seconds:
            older_5y += 1
        if age_seconds > ten_years_seconds:
            older_10y += 1

    if progress_label:
        progress_print(
            f"finished stat aggregation {progress_label}; valid_files={len(valid_files):,}; "
            f"elapsed_seconds={time.time() - meta_started:,.1f}"
        )

    if not valid_files:
        return {
            "folder_size_bytes": 0,
            "largest_file_bytes": 0,
            "num_files_total": total_count,
            "newest_file_mtime_utc": "",
            "oldest_file_mtime_utc": "",
            "percent_files_older_5y": "",
            "percent_files_older_10y": "",
            "top_extensions_csv": "",
            "is_geospatial": False,
            "is_document": False,
            "is_software": False,
            "num_duplicates": 0,
            "duplicate_ids": "",
        }

    sampled_total_size = sum(sizes)
    sampled_count = len(valid_files)

    if total_count <= sampled_count:
        estimated_folder_size = sampled_total_size
    else:
        estimated_folder_size = int((sampled_total_size / sampled_count) * total_count)

    top_extensions_csv = "; ".join(
        f"{ext}:{count}" for ext, count in extension_counter.most_common(10)
    )

    duplicate_ids = ""
    num_duplicates = 0

    perf_cfg = cfg.get("performance", {})
    duplicate_mode = str(perf_cfg.get("duplicate_detection_mode", "")).strip().lower()

    # Backward compatibility for older config files that only had duplicate_detection.
    if not duplicate_mode:
        duplicate_mode = "full" if perf_cfg.get("duplicate_detection", False) else "off"
    if duplicate_mode in {"false", "none", "no", "0"}:
        duplicate_mode = "off"
    if duplicate_mode in {"true", "yes", "1"}:
        duplicate_mode = "full"

    if duplicate_mode in {"full", "sample", "sampled"}:
        if duplicate_mode == "full":
            files_to_hash = list(valid_files)
            byte_count = int(perf_cfg.get("md5_first_bytes", 1024 * 1024))
            duplicate_note = "full"
        else:
            # Sample mode avoids opening every file. It first finds cheap candidate
            # groups based on exact file size + extension, then hashes only a capped
            # subset using a small byte read. This is only a duplicate-risk sample.
            max_sample_files = int(perf_cfg.get("duplicate_sample_max_files", 200))
            byte_count = int(perf_cfg.get("duplicate_sample_first_bytes", 4096))
            candidate_groups: Dict[Tuple[int, str], List[Path]] = defaultdict(list)
            for f, size in zip(valid_files, sizes):
                candidate_groups[(size, f.suffix.lower())].append(f)

            candidate_files: List[Path] = []
            for _, group in sorted(
                candidate_groups.items(),
                key=lambda item: len(item[1]),
                reverse=True,
            ):
                if len(group) > 1:
                    candidate_files.extend(group)
                if len(candidate_files) >= max_sample_files:
                    break

            files_to_hash = candidate_files[:max_sample_files]
            duplicate_note = (
                f"sample; candidates_by_same_size_ext={len(candidate_files):,}; "
                f"sample_cap={max_sample_files:,}"
            )

        hashes: Dict[str, List[str]] = defaultdict(list)

        hash_started = time.time()
        last_hash_print = hash_started
        if progress_label:
            progress_print(
                f"hashing for duplicates {progress_label}; mode={duplicate_note}; "
                f"files_to_hash={len(files_to_hash):,}; bytes_per_file={byte_count:,}"
            )

        for hash_index, f in enumerate(files_to_hash, start=1):
            now_for_progress = time.time()
            should_print_by_count = progress_every_files > 0 and hash_index % progress_every_files == 0
            should_print_by_time = heartbeat_seconds > 0 and (now_for_progress - last_hash_print) >= heartbeat_seconds
            if progress_label and (should_print_by_count or should_print_by_time):
                progress_print(
                    f"hashing for duplicates {progress_label}; hashed={hash_index:,}/{len(files_to_hash):,}; "
                    f"errors={len(stats.pruned_directories) + len(stats.file_errors):,}; current={f}"
                )
                last_hash_print = now_for_progress

            file_hash = md5_first_bytes(f, byte_count, stats)
            if file_hash:
                hashes[file_hash].append(str(f))

        if progress_label:
            progress_print(
                f"finished duplicate hashing {progress_label}; mode={duplicate_mode}; hashed={len(files_to_hash):,}; "
                f"elapsed_seconds={time.time() - hash_started:,.1f}"
            )

        duplicate_groups = {
            h: paths for h, paths in hashes.items()
            if len(paths) > 1
        }

        num_duplicates = sum(len(paths) for paths in duplicate_groups.values())
        duplicate_ids = "; ".join(
            f"{h}:{len(paths)}" for h, paths in duplicate_groups.items()
        )
    elif duplicate_mode != "off" and progress_label:
        progress_print(
            f"unknown duplicate_detection_mode={duplicate_mode!r}; skipping duplicate detection {progress_label}"
        )

    return {
        "folder_size_bytes": estimated_folder_size,
        "largest_file_bytes": max(sizes),
        "num_files_total": total_count,
        "newest_file_mtime_utc": mtime_to_utc_iso(max(mtimes)),
        "oldest_file_mtime_utc": mtime_to_utc_iso(min(mtimes)),
        "percent_files_older_5y": round((older_5y / sampled_count) * 100, 2),
        "percent_files_older_10y": round((older_10y / sampled_count) * 100, 2),
        "top_extensions_csv": top_extensions_csv,
        "is_geospatial": is_geospatial,
        "is_document": is_document,
        "is_software": is_software,
        "num_duplicates": num_duplicates,
        "duplicate_ids": duplicate_ids,
    }


def full_scan(
    folder: Path,
    cfg: Dict[str, Any],
    stats: ScanStats,
    progress_label: str = "",
    progress_every_dirs: int = 500,
    progress_every_files: int = 500,
    heartbeat_seconds: int = 30,
) -> Dict[str, Any]:
    full_file_list = collect_recursive_files(
        folder,
        stats,
        progress_label=progress_label,
        progress_every_dirs=progress_every_dirs,
        heartbeat_seconds=heartbeat_seconds,
    )
    return aggregate_files(
        full_file_list,
        len(full_file_list),
        cfg,
        stats,
        progress_label=progress_label,
        progress_every_files=progress_every_files,
        heartbeat_seconds=heartbeat_seconds,
    )


def sampled_scan(
    folder: Path,
    direct_files: Sequence[Path],
    cfg: Dict[str, Any],
    stats: ScanStats,
    progress_label: str = "",
    progress_every_dirs: int = 500,
    progress_every_files: int = 500,
    heartbeat_seconds: int = 30,
) -> Dict[str, Any]:
    """
    Preserves the documented sampled scan behavior:
    - If direct files are <= total_sampled_files, use immediate files.
    - Otherwise collect descendant files and choose a sample.
    - total_count passed to aggregate_files remains the direct file count.
    """
    direct_file_count = len(direct_files)
    total_sampled_files = int(cfg["sampling"]["total_sampled_files"])

    if direct_file_count <= total_sampled_files:
        sampled = list(direct_files)
    else:
        all_files = collect_recursive_files(
            folder,
            stats,
            progress_label=progress_label,
            progress_every_dirs=progress_every_dirs,
            heartbeat_seconds=heartbeat_seconds,
        )
        sampled = sample_files(all_files, cfg, stats)

    return aggregate_files(
        sampled,
        direct_file_count,
        cfg,
        stats,
        progress_label=progress_label,
        progress_every_files=progress_every_files,
        heartbeat_seconds=heartbeat_seconds,
    )


# -----------------------------
# Row and output helpers
# -----------------------------

def find_root_for_path(path: Path, roots: Sequence[Path], labels: Sequence[str]) -> Tuple[Path, str]:
    """
    Finds the matching root for a queued path.

    Longest matching root wins, which makes this safe if roots overlap.
    """
    path_resolved = path.resolve()
    best_root = roots[0]
    best_label = labels[0]
    best_len = -1

    for root, label in zip(roots, labels):
        try:
            root_resolved = root.resolve()
            path_resolved.relative_to(root_resolved)
            root_len = len(str(root_resolved))
            if root_len > best_len:
                best_root = root
                best_label = label
                best_len = root_len
        except ValueError:
            continue

    return best_root, best_label


def relative_path_and_depth(path: Path, root: Path) -> Tuple[str, int]:
    try:
        rel = path.resolve().relative_to(root.resolve())
        rel_str = "." if str(rel) == "." else str(rel)
        depth = 0 if rel_str == "." else len(rel.parts)
        return rel_str, depth
    except Exception:
        return str(path), 0


def build_folder_row(
    root_label: str,
    folder: Path,
    rel_path: str,
    depth: int,
    direct_subfolder_count: int,
    direct_file_count: int,
    scan_mode: str,
    agg: Dict[str, Any],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    row = {
        "root_label": root_label,
        "path": str(folder),
        "rel_path": rel_path,
        "depth": depth,
        "direct_subfolders": direct_subfolder_count,
        "direct_files": direct_file_count,
        "scan_mode": scan_mode,
    }

    row.update(agg)

    for col in cfg["output"]["human_classification_columns"]:
        row[col] = ""

    return row


def prepare_csv_writer(
    csv_path: Path,
    resume_mode: bool,
    cfg: Dict[str, Any],
) -> Tuple[Any, csv.DictWriter]:
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    open_mode = "a" if resume_mode else "w"
    file_exists = csv_path.exists()
    file_has_content = file_exists and csv_path.stat().st_size > 0

    f = csv_path.open(open_mode, newline="", encoding="utf-8-sig")

    fieldnames = CSV_FIELDNAMES + cfg["output"]["human_classification_columns"]
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")

    if not resume_mode or not file_has_content:
        writer.writeheader()
        f.flush()

    return f, writer


def write_tree_txt(tree_path: Path, tree_rows: Sequence[Dict[str, Any]]) -> None:
    lines: List[str] = []

    current_label = None
    for row in tree_rows:
        label = row["root_label"]
        if label != current_label:
            if lines:
                lines.append("")
            lines.append(f"== ROOT: {label} ==")
            current_label = label

        depth = int(row["depth"])
        name = "." if row["rel_path"] == "." else Path(row["path"]).name + "/"
        indent = "    " * depth
        connector = "" if depth == 0 else "└── "

        lines.append(
            f"{indent}{connector}{name} "
            f"({row['direct_subfolders']} subfolders, {row['direct_files']} files)"
        )

    tree_path.write_text("\n".join(lines), encoding="utf-8")


def write_summary_json(
    summary_path: Path,
    roots: Sequence[Path],
    labels: Sequence[str],
    stats: ScanStats,
    cfg_hash: str,
) -> None:
    summary = {
        "generated_utc": utc_now_iso(),
        "roots_scanned": [str(r) for r in roots],
        "root_labels": list(labels),
        "config_hash": cfg_hash,
        "totals": {
            "folders": stats.rows_written,
            "pruned_directories": len(stats.pruned_directories),
            "file_errors": len(stats.file_errors),
        },
        "pruned_directories": stats.pruned_directories,
        "file_errors_sample": dict(list(stats.file_errors.items())[:500]),
    }
    write_json_atomic(summary_path, summary)


# -----------------------------
# Checkpoint startup logic
# -----------------------------

def initialize_resume_state(
    checkpoint_path: Path,
    cfg_hash: str,
    roots: Sequence[Path],
    labels: Sequence[str],
) -> Tuple[bool, set, Optional[List[Path]]]:
    if not checkpoint_path.exists():
        return False, set(), None

    answer = input("Checkpoint exists. Resume previous scan? (y/N): ").strip().lower()

    if answer != "y":
        checkpoint_path.unlink(missing_ok=True)
        return False, set(), None

    cp = load_checkpoint(checkpoint_path)
    if not cp:
        print("Checkpoint could not be read. Restarting scan.")
        checkpoint_path.unlink(missing_ok=True)
        return False, set(), None

    expected_roots = [str(r) for r in roots]
    expected_labels = list(labels)

    compatible = (
        cp.get("config_hash") == cfg_hash
        and cp.get("roots") == expected_roots
        and cp.get("root_labels") == expected_labels
    )

    if not compatible:
        print("Checkpoint is incompatible with current config/root/label. Restarting scan.")
        checkpoint_path.unlink(missing_ok=True)
        return False, set(), None

    completed_paths = set(cp.get("completed_paths", []))
    initial_queue = [Path(p) for p in cp.get("remaining_queue", [])]

    return True, completed_paths, initial_queue


# -----------------------------
# Main BFS scan loop
# -----------------------------

def run_scan(
    roots: Sequence[Path],
    labels: Sequence[str],
    out_dir: Path,
    cfg: Dict[str, Any],
    cfg_hash: str,
    max_depth: int,
    resume_mode: bool,
    completed_paths: set,
    initial_queue: Optional[Sequence[Path]],
    progress_every: int = 25,
    walk_progress_every_dirs: int = 500,
    file_progress_every: int = 500,
    heartbeat_seconds: int = 30,
) -> None:
    stats = ScanStats()

    checkpoint_path = out_dir / "checkpoint.json"
    csv_path = out_dir / "folder_inventory.csv"
    tree_path = out_dir / "tree.txt"
    summary_path = out_dir / "summary.json"

    queue = deque(initial_queue if resume_mode and initial_queue is not None else roots)

    csv_file, writer = prepare_csv_writer(csv_path, resume_mode, cfg)

    checkpoint_frequency = int(cfg["performance"]["checkpoint_frequency"])
    csv_flush_frequency = int(cfg["performance"]["csv_flush_frequency"])

    tree_rows: List[Dict[str, Any]] = []
    scanned_since_start = 0
    start_time = time.time()
    last_loop_print = start_time

    progress_print(
        f"starting scan; roots={len(roots):,}; initial_queue={len(queue):,}; "
        f"resume={resume_mode}; max_depth={max_depth}; out={out_dir}"
    )

    try:
        while queue:
            current = queue.popleft()
            current_str = str(current)

            if current_str in completed_paths:
                continue

            root, label = find_root_for_path(current, roots, labels)
            rel_path, depth = relative_path_and_depth(current, root)

            subdirs, direct_files = safe_scandir(current, stats)
            direct_subfolder_count = len(subdirs)
            direct_file_count = len(direct_files)

            scan_mode = choose_scan_mode(current_str, direct_file_count, cfg)

            now = time.time()
            should_print_progress = (
                (progress_every > 0 and (stats.rows_written + 1) % progress_every == 0)
                or (heartbeat_seconds > 0 and (now - last_loop_print) >= heartbeat_seconds)
                or stats.rows_written == 0
            )
            if should_print_progress:
                progress_print(
                    f"processing folder #{stats.rows_written + 1:,}; mode={scan_mode}; depth={depth}; "
                    f"queue={len(queue):,}; subfolders={direct_subfolder_count:,}; "
                    f"direct_files={direct_file_count:,}; path={current}"
                )
                last_loop_print = now

            progress_label = f"{scan_mode} folder #{stats.rows_written + 1:,}: {current}"

            if scan_mode == "FULL":
                agg = full_scan(
                    current,
                    cfg,
                    stats,
                    progress_label=progress_label,
                    progress_every_dirs=walk_progress_every_dirs,
                    progress_every_files=file_progress_every,
                    heartbeat_seconds=heartbeat_seconds,
                )
            else:
                agg = sampled_scan(
                    current,
                    direct_files,
                    cfg,
                    stats,
                    progress_label=progress_label,
                    progress_every_dirs=walk_progress_every_dirs,
                    progress_every_files=file_progress_every,
                    heartbeat_seconds=heartbeat_seconds,
                )

            row = build_folder_row(
                root_label=label,
                folder=current,
                rel_path=rel_path,
                depth=depth,
                direct_subfolder_count=direct_subfolder_count,
                direct_file_count=direct_file_count,
                scan_mode=scan_mode,
                agg=agg,
                cfg=cfg,
            )

            writer.writerow(row)
            tree_rows.append(row)

            completed_paths.add(current_str)
            stats.rows_written += 1
            scanned_since_start += 1

            if depth < max_depth:
                queue.extend(subdirs)

            if scanned_since_start % csv_flush_frequency == 0:
                csv_file.flush()

            if scanned_since_start % checkpoint_frequency == 0:
                write_checkpoint(
                    checkpoint_path,
                    completed_paths,
                    list(queue),
                    cfg_hash,
                    roots,
                    labels,
                )
                progress_print(
                    f"checkpoint saved; scanned={stats.rows_written:,} folders; "
                    f"queue={len(queue):,}; errors={len(stats.pruned_directories) + len(stats.file_errors):,}"
                )

    finally:
        csv_file.flush()
        csv_file.close()

    if checkpoint_path.exists():
        checkpoint_path.unlink()

    progress_print("writing tree.txt and summary.json")
    write_tree_txt(tree_path, tree_rows)
    write_summary_json(summary_path, roots, labels, stats, cfg_hash)

    progress_print(f"done; elapsed_seconds={time.time() - start_time:,.1f}")
    print(f"Done. Wrote: {csv_path}", flush=True)
    print(f"Tree: {tree_path}", flush=True)
    print(f"Summary: {summary_path}", flush=True)


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan shared-drive folders for cloud migration planning."
    )

    parser.add_argument(
        "--config",
        required=True,
        help="Path to config.yaml",
    )

    parser.add_argument(
        "--root",
        required=True,
        action="append",
        help="Root folder to scan. Can be supplied multiple times.",
    )

    parser.add_argument(
        "--label",
        action="append",
        help="Root label. If omitted, folder name is used. Can be supplied multiple times.",
    )

    parser.add_argument(
        "--out",
        required=True,
        help="Output directory.",
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help="Maximum folder depth to scan below each root.",
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Print folder-loop progress every N folders. Use 0 to disable count-based progress.",
    )

    parser.add_argument(
        "--walk-progress-every-dirs",
        type=int,
        default=500,
        help="During recursive folder walks, print progress every N directories walked. Use 0 to disable.",
    )

    parser.add_argument(
        "--file-progress-every",
        type=int,
        default=500,
        help="During file stat/hash aggregation, print progress every N files. Use 0 to disable count-based file progress.",
    )

    parser.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=30,
        help="Print a progress heartbeat at least this often during long folder walks or file aggregation. Use 0 to disable.",
    )

    parser.add_argument(
        "--duplicate-mode",
        choices=["off", "sample", "full"],
        default=None,
        help=(
            "Duplicate detection mode. off is recommended for first-pass mapping; "
            "sample checks a capped candidate set; full hashes every checked file."
        ),
    )

    parser.add_argument(
        "--duplicate-sample-max-files",
        type=int,
        default=None,
        help="In duplicate sample mode, maximum number of candidate files to hash.",
    )

    parser.add_argument(
        "--duplicate-sample-first-bytes",
        type=int,
        default=None,
        help="In duplicate sample mode, number of bytes to hash from each sampled candidate file.",
    )

    parser.add_argument(
        "--no-duplicate-detection",
        action="store_true",
        help="Backward-compatible alias for --duplicate-mode off.",
    )

    return parser.parse_args()


def derive_labels(roots: Sequence[Path], labels_arg: Optional[Sequence[str]]) -> List[str]:
    if labels_arg is None:
        return [r.name or str(r) for r in roots]

    if len(labels_arg) != len(roots):
        raise ValueError(
            "Number of --label values must match number of --root values, "
            "or omit --label entirely."
        )

    return list(labels_arg)


def main() -> None:
    args = parse_args()

    config_path = Path(args.config)
    out_dir = Path(args.out)
    roots = [Path(r) for r in args.root]
    labels = derive_labels(roots, args.label)

    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = load_config(config_path)
    cfg_hash = compute_config_hash(config_path)

    max_depth = (
        args.max_depth
        if args.max_depth is not None
        else int(cfg.get("max_depth", 999999))
    )

    checkpoint_path = out_dir / "checkpoint.json"

    resume_mode, completed_paths, initial_queue = initialize_resume_state(
        checkpoint_path=checkpoint_path,
        cfg_hash=cfg_hash,
        roots=roots,
        labels=labels,
    )

    if args.no_duplicate_detection:
        cfg.setdefault("performance", {})["duplicate_detection_mode"] = "off"
        cfg.setdefault("performance", {})["duplicate_detection"] = False
        progress_print("duplicate detection disabled for this run")

    if args.duplicate_mode is not None:
        cfg.setdefault("performance", {})["duplicate_detection_mode"] = args.duplicate_mode
        cfg.setdefault("performance", {})["duplicate_detection"] = args.duplicate_mode == "full"
        progress_print(f"duplicate detection mode set to {args.duplicate_mode}")

    if args.duplicate_sample_max_files is not None:
        cfg.setdefault("performance", {})["duplicate_sample_max_files"] = args.duplicate_sample_max_files

    if args.duplicate_sample_first_bytes is not None:
        cfg.setdefault("performance", {})["duplicate_sample_first_bytes"] = args.duplicate_sample_first_bytes

    run_scan(
        roots=roots,
        labels=labels,
        out_dir=out_dir,
        cfg=cfg,
        cfg_hash=cfg_hash,
        max_depth=max_depth,
        resume_mode=resume_mode,
        completed_paths=completed_paths,
        initial_queue=initial_queue,
        progress_every=args.progress_every,
        walk_progress_every_dirs=args.walk_progress_every_dirs,
        file_progress_every=args.file_progress_every,
        heartbeat_seconds=args.heartbeat_seconds,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(130)
