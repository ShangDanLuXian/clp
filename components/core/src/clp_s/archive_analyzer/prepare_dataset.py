#!/usr/bin/env python3
"""Streams a .tar.gz of newline-delimited JSON log files into fixed-size line-aligned parts,
compresses each part into its own clp-s archive (preserving the original time order via part
sequence numbers), and deletes raw parts as soon as they are compressed so the whole dataset never
needs to fit on disk.

Usage: prepare_dataset.py TARBALL OUT_DIR --clp-s PATH [--part-bytes N] [--jobs J]
"""

import argparse
import os
import queue
import shutil
import subprocess
import sys
import tarfile
import threading

def compress_worker(
    work_queue: "queue.Queue[str]",
    archives_dir: str,
    clp_s: str,
    extra_args: list,
    failures: list,
) -> None:
    while True:
        part_path = work_queue.get()
        if part_path is None:
            work_queue.task_done()
            return
        part_name = os.path.basename(part_path)
        archive_dir = os.path.join(archives_dir, part_name)
        os.makedirs(archive_dir, exist_ok=True)
        result = subprocess.run(
            [clp_s, "c", archive_dir, part_path, *extra_args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if 0 != result.returncode:
            failures.append((part_name, result.stderr.decode(errors="replace")[-500:]))
            shutil.rmtree(archive_dir, ignore_errors=True)
        os.remove(part_path)
        work_queue.task_done()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("tarball")
    parser.add_argument("out_dir")
    parser.add_argument("--clp-s", required=True, dest="clp_s")
    parser.add_argument("--part-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--jobs", type=int, default=3)
    parser.add_argument("--target-encoded-size", type=int, default=8 * 1024 * 1024 * 1024)
    parser.add_argument("--timestamp-key", default=None)
    parser.add_argument(
        "--max-staged", type=int, default=32, help="Backpressure: max pending parts."
    )
    args = parser.parse_args()

    staging_dir = os.path.join(args.out_dir, "staging")
    archives_dir = os.path.join(args.out_dir, "archives")
    os.makedirs(staging_dir, exist_ok=True)
    os.makedirs(archives_dir, exist_ok=True)

    extra_args = ["--target-encoded-size", str(args.target_encoded_size)]
    if args.timestamp_key:
        extra_args.extend(["--timestamp-key", args.timestamp_key])

    work_queue: "queue.Queue[str]" = queue.Queue()
    failures: list = []
    workers = [
        threading.Thread(
            target=compress_worker,
            args=(work_queue, archives_dir, args.clp_s, extra_args, failures),
            daemon=True,
        )
        for _ in range(args.jobs)
    ]
    for worker in workers:
        worker.start()

    part_idx = 0

    def submit_part(buffer: bytes) -> None:
        nonlocal part_idx
        # Backpressure: don't let raw parts pile up faster than they're compressed.
        while work_queue.qsize() > args.max_staged:
            import time

            time.sleep(2)
        part_path = os.path.join(staging_dir, f"part_{part_idx:06d}")
        with open(part_path, "wb") as part_file:
            part_file.write(buffer)
        work_queue.put(part_path)
        part_idx += 1
        if 0 == part_idx % 50:
            print(f"submitted {part_idx} parts...", file=sys.stderr, flush=True)

    with tarfile.open(args.tarball, "r|gz") as tar:
        pending = b""
        for member in tar:
            if not member.isfile():
                continue
            stream = tar.extractfile(member)
            if stream is None:
                continue
            while True:
                chunk = stream.read(8 * 1024 * 1024)
                if not chunk:
                    break
                pending += chunk
                while len(pending) >= args.part_bytes:
                    cut = pending.rfind(b"\n", 0, args.part_bytes)
                    if cut < 0:
                        cut = args.part_bytes
                    submit_part(pending[: cut + 1])
                    pending = pending[cut + 1 :]
        if pending.strip():
            submit_part(pending)

    work_queue.join()
    for _ in workers:
        work_queue.put(None)
    for worker in workers:
        worker.join()

    print(f"done: {part_idx} parts -> {archives_dir}", file=sys.stderr)
    if failures:
        print(f"{len(failures)} parts FAILED to compress:", file=sys.stderr)
        for name, err in failures[:10]:
            print(f"  {name}: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
