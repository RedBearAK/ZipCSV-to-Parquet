"""
zipcsv2parquet - every csv inside a zip becomes a zstd Parquet, without extracting.

zipcsv2parquet/__main__.py

    zipcsv2parquet FILE.zip [FILE.zip ...] [--out-dir DIR] [--level N]
                   [--encoding ENC] [--delimiter D] [--infer-types] [--overwrite]

Prints the Parquet paths it wrote, one per line, on STDOUT - that is the
contract a wrapping script relies on. Everything else (progress, what
was ignored, errors) goes to STDERR. Exit 0 when every archive produced
at least one file; 1 otherwise.

One Parquet per csv member, named for the member, beside the archive
unless --out-dir says otherwise. Columns are text unless --infer-types.
A csv that does not parse cleanly is refused and leaves nothing behind.
"""

import os
import sys
import argparse

from zipcsv2parquet._version import __version__
from zipcsv2parquet.convert import COMPRESSION_LEVEL, ConvertError, convert_archive


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='zipcsv2parquet', description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--version', action='version', version=f"zipcsv2parquet {__version__}")
    parser.add_argument('archives', nargs='+', help='zip file(s) holding csv member(s)')
    parser.add_argument('--out-dir', default='', help='where the Parquet goes (default: beside each archive)')
    parser.add_argument('--level', type=int, default=COMPRESSION_LEVEL,
                        help=f'zstd level, 1 fast .. 19 small (default {COMPRESSION_LEVEL})')
    parser.add_argument('--encoding', default='utf8', help='csv text encoding (default utf8; a BOM is fine)')
    parser.add_argument('--delimiter', default=',', help="field delimiter (default ',')")
    parser.add_argument('--infer-types', action='store_true',
                        help='let pyarrow infer column types instead of keeping every column as text')
    parser.add_argument('--overwrite', action='store_true', help='replace a Parquet that already exists')
    parser.add_argument('--undoubled-quotes', action='store_true',
                        help='the exporter never doubles a quote inside a field, so "" can only be an inner '
                             'quote followed by the closing one (a value ending in an inch mark); off, "" is '
                             'read as standard csv escaping')
    return parser


def main(argv: list = None) -> int:
    arguments = build_parser().parse_args(argv)
    failures = 0
    for archive_path in arguments.archives:
        out_dir = arguments.out_dir or os.path.dirname(os.path.abspath(archive_path))
        try:
            written = convert_archive(
                archive_path, out_dir, level=arguments.level, encoding=arguments.encoding,
                delimiter=arguments.delimiter, infer_types=arguments.infer_types,
                overwrite=arguments.overwrite, undoubled_quotes=arguments.undoubled_quotes,
                report=lambda message: print(f"  {message}", file=sys.stderr))
        except (ConvertError, OSError) as error:
            print(f"error: {error}", file=sys.stderr)
            failures += 1
            continue
        for path in written:
            print(path)
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())


# End of file #
