"""
Cut a Parquet file into parts of at most N rows, at exactly N.

zipcsv2parquet/split.py

    parquetsplit FILE.parquet [--rows N] [--out-dir DIR] [--overwrite]

Parts are FILE_part01.parquet, FILE_part02.parquet, ... beside the file
(or in --out-dir), each holding at most --rows rows, the schema and
compression of the original, every row in exactly one part, in order.
The paths written go to stdout one per line, the same contract as
zipcsv2parquet, so a script loops over them the same way.

The default --rows is 250,000: a workbook that writes in a fraction
of the time a million-row one takes and opens and filters comfortably.
Excel's limit is 1,048,576 rows per sheet, and people do work with
sheets that size, but that should be asked for (Kris 2026-09-14):
--rows 1000000. A file with no more rows than the size is not split -
it is printed as its own single "part", so a caller need not
special-case the small file.

The cut is at exactly N rows, not at the file's row-group seams.
Splitting at a seam would avoid re-encoding one chunk per cut and
produce parts sized by how the file happened to be written, which is
an artificial place to put a boundary a person asked for by number
(Kris 2026-09-14). Rows are read in batches and written in row groups
of the same size, so memory is bounded whatever the file's size.
"""

import os
import sys
import argparse

import pyarrow.parquet as pq

from zipcsv2parquet._version import __version__
from zipcsv2parquet.convert import COMPRESSION, COMPRESSION_LEVEL, ROW_GROUP_ROWS


DEFAULT_ROWS = 250_000


class SplitError(Exception):
    pass


def part_path(source: str, out_dir: str, index: int, width: int) -> str:
    stem = os.path.splitext(os.path.basename(source))[0]
    return os.path.join(out_dir, f"{stem}_part{index:0{width}d}.parquet")


def split_parquet(source: str, out_dir: str, rows_per_part: int = DEFAULT_ROWS, overwrite: bool = False,
                  report=None) -> list:
    """Write the parts; return their paths in order. A file that fits in
    one part is returned as [source] and nothing is written."""
    say = report or (lambda message: None)
    if rows_per_part < 1:
        raise SplitError(f"--rows must be at least 1, got {rows_per_part}")
    metadata = pq.read_metadata(source)
    total = metadata.num_rows
    if total <= rows_per_part:
        say(f"unsplit    {os.path.basename(source)!r}: {total:,} rows fit in one part of {rows_per_part:,}")
        return [source]
    count = (total + rows_per_part - 1) // rows_per_part
    width = max(2, len(str(count)))
    os.makedirs(out_dir, exist_ok=True)
    paths = [part_path(source, out_dir, index, width) for index in range(1, count + 1)]
    for path in paths:
        if os.path.exists(path) and not overwrite:
            raise SplitError(f"{path!r} exists; use --overwrite to replace the parts")

    reader = pq.ParquetFile(source)
    codec = metadata.row_group(0).column(0).compression.lower() if metadata.num_row_groups else COMPRESSION
    written = []
    part_index = 0
    writer = None
    in_part = 0
    try:
        for batch in reader.iter_batches(batch_size=min(ROW_GROUP_ROWS, rows_per_part)):
            offset = 0
            while offset < batch.num_rows:
                if writer is None:
                    part_index += 1
                    writer = pq.ParquetWriter(paths[part_index - 1] + '.partial', reader.schema_arrow,
                                              compression=codec, compression_level=COMPRESSION_LEVEL)
                    in_part = 0
                take = min(batch.num_rows - offset, rows_per_part - in_part)
                writer.write_batch(batch.slice(offset, take))
                offset += take
                in_part += take
                if in_part == rows_per_part:
                    writer.close()
                    writer = None
                    os.replace(paths[part_index - 1] + '.partial', paths[part_index - 1])
                    written.append(paths[part_index - 1])
                    say(f"wrote      {os.path.basename(paths[part_index - 1])!r}: {in_part:,} rows")
        if writer is not None:
            writer.close()
            writer = None
            os.replace(paths[part_index - 1] + '.partial', paths[part_index - 1])
            written.append(paths[part_index - 1])
            say(f"wrote      {os.path.basename(paths[part_index - 1])!r}: {in_part:,} rows")
    finally:
        if writer is not None:
            writer.close()
        for path in paths:
            if os.path.exists(path + '.partial'):
                os.remove(path + '.partial')
    if sum(pq.read_metadata(path).num_rows for path in written) != total:
        raise SplitError(f"the parts hold a different row count than the source; parts removed")
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='parquetsplit', description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--version', action='version', version=f"parquetsplit {__version__}")
    parser.add_argument('source', help='the Parquet file to split')
    parser.add_argument('--rows', type=int, default=DEFAULT_ROWS,
                        help=f'rows per part (default {DEFAULT_ROWS:,}: a workbook that writes and opens quickly; '
                             f'up to 1000000 fits one Excel sheet, if that is what you want)')
    parser.add_argument('--out-dir', default='', help='where the parts go (default: beside the file)')
    parser.add_argument('--overwrite', action='store_true', help='replace existing parts')
    return parser


def main(argv: list = None) -> int:
    arguments = build_parser().parse_args(argv)
    if not os.path.isfile(arguments.source):
        print(f"error: {arguments.source!r} is not a file", file=sys.stderr)
        return 2
    out_dir = arguments.out_dir or os.path.dirname(os.path.abspath(arguments.source))
    try:
        paths = split_parquet(arguments.source, out_dir, arguments.rows, arguments.overwrite,
                              report=lambda message: print(f"  {message}", file=sys.stderr))
    except (SplitError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    for path in paths:
        print(path)
    return 0


if __name__ == '__main__':
    sys.exit(main())


# End of file #
