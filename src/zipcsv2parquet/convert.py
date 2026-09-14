"""
Every csv inside a zip becomes a Parquet file, without extracting anything.

zipcsv2parquet/convert.py

Reads each csv member as a stream straight out of the archive and writes
it as zstd-compressed Parquet, all columns text, in row groups, so a
600 MB csv that zips to 10 MB never exists on disk as 600 MB of
anything. One Parquet per csv member, named for the member. Non-csv
members (README, manifest) are listed and ignored. A csv that does not
parse cleanly is refused and no partial file is left behind: an
archive from a system export should be complete, and a tool that
silently salvaged a broken one would hide that it was broken.

Types are text on purpose. Package numbers, lot numbers and sequence
numbers all look numeric and are not; a downstream recipe types what it
needs. --infer-types is the opt-in.

One repair IS made, because it is a property of complete files rather
than broken ones: an inner quote the export forgot to double (a note
like  40" high  inside a quoted field) is doubled on the way through
(inner_quotes.py). Without it a strict parser
closes the field at that quote and every line break inside a note
becomes a record boundary. The count of repairs is reported.
"""

import io
import os
import zipfile

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from zipcsv2parquet.convert_rgx import csv_member_rgx, junk_member_rgx
from zipcsv2parquet.inner_quotes import InnerQuoteRepair
from zipcsv2parquet.diagnose import Finding, Progress, locate_bad_record


COMPRESSION = 'zstd'
COMPRESSION_LEVEL = 15
ROW_GROUP_ROWS = 262_144


class ConvertError(Exception):
    pass


class Member:
    """One csv member of an archive and where its Parquet goes."""

    def __init__(self, name: str, size: int, out_path: str):
        self.name = name
        self.size = size
        self.out_path = out_path


def plan(archive_path: str, out_dir: str) -> tuple:
    """(members to convert, members ignored). Refuses two csv members that
    would produce the same Parquet name - the archive's layout, not the
    tool's naming, has to decide that."""
    with zipfile.ZipFile(archive_path) as archive:
        infos = [info for info in archive.infolist() if not info.is_dir()]
    members = []
    ignored = []
    seen = {}
    for info in infos:
        base = os.path.basename(info.filename)
        if junk_member_rgx.search(info.filename):
            continue                        # Finder's leavings: not worth a line
        if not csv_member_rgx.search(base):
            ignored.append(info.filename)
            continue
        stem = os.path.splitext(base)[0]
        out_path = os.path.join(out_dir, stem + '.parquet')
        if out_path in seen:
            raise ConvertError(
                f"two csv members would both become {os.path.basename(out_path)!r}: "
                f"{seen[out_path]!r} and {info.filename!r}")
        seen[out_path] = info.filename
        members.append(Member(info.filename, info.file_size, out_path))
    return members, ignored


def column_names(archive: zipfile.ZipFile, member: Member, encoding: str, delimiter: str,
                 undoubled_quotes: bool = False, limit: int = 0) -> list:
    """The header as pyarrow reads it - quoted names and a BOM handled -
    from a first, cheap open that reads only the first block."""
    # pyarrow reads a whole first block to find the header, so a fault
    # early in the file surfaces HERE rather than in the stream; the caller
    # catches and diagnoses either the same way
    with archive.open(member.name) as stream:
        reader = pcsv.open_csv(
            InnerQuoteRepair(stream, delimiter.encode('ascii'), undoubled_quotes, limit),
            read_options=pcsv.ReadOptions(encoding=encoding),
            parse_options=pcsv.ParseOptions(delimiter=delimiter, newlines_in_values=True),
        )
        return list(reader.schema.names)


def located(archive, member, encoding, delimiter, undoubled_quotes) -> Finding:
    """Where the first bad record is - and never an error of its own, since
    the diagnosis must not hide the failure it explains."""
    try:
        return locate_bad_record(archive, member.name, encoding, delimiter, undoubled_quotes)
    except Exception as locate_error:
        return Finding(f"(could not locate the record: {locate_error})")


def refusal(member, error, finding: Finding) -> ConvertError:
    text = f"{member.name!r} does not parse cleanly: {error}"
    if finding.description:
        text += f"\n{finding.description}"
    return ConvertError(text)


def convert_member(archive: zipfile.ZipFile, member: Member, level: int, encoding: str,
                   delimiter: str, infer_types: bool, undoubled_quotes: bool = False,
                   strict: bool = False, limit: int = 0) -> tuple:
    """Stream one member to its Parquet. Returns (row count, inner quotes
    repaired, note). A truncated tail - the LAST record short, with no
    final newline, which nothing but a download that stopped mid-record
    produces - is dropped and reported, unless strict; any other failure
    removes the partial output and raises ConvertError."""
    try:
        names = column_names(archive, member, encoding, delimiter, undoubled_quotes, limit)
    except (pa.ArrowInvalid, pa.ArrowException, UnicodeDecodeError, ValueError) as error:
        finding = located(archive, member, encoding, delimiter, undoubled_quotes)
        if finding.truncated_tail and not strict and not limit:
            return retry_without_tail(archive, member, level, encoding, delimiter, infer_types,
                                      undoubled_quotes, finding)
        raise refusal(member, error, finding) from error
    if len(set(names)) != len(names):
        duplicates = sorted({name for name in names if names.count(name) > 1})
        raise ConvertError(f"{member.name!r}: duplicate column names {duplicates}")
    convert = pcsv.ConvertOptions(null_values=[''], strings_can_be_null=True)
    if not infer_types:
        convert = pcsv.ConvertOptions(column_types={name: pa.string() for name in names},
                                      null_values=[''], strings_can_be_null=True)
    rows = 0
    temp_path = member.out_path + '.partial'
    writer = None
    repair = None
    try:
        with archive.open(member.name) as stream:
            repair = InnerQuoteRepair(stream, delimiter.encode('ascii'), undoubled_quotes, limit)
            reader = pcsv.open_csv(
                repair,
                read_options=pcsv.ReadOptions(encoding=encoding, block_size=8 << 20),
                parse_options=pcsv.ParseOptions(delimiter=delimiter, newlines_in_values=True),
                convert_options=convert,
            )
            writer = pq.ParquetWriter(temp_path, reader.schema, compression=COMPRESSION,
                                      compression_level=level)
            progress = Progress(os.path.basename(member.name))
            pending = []
            pending_rows = 0
            for batch in reader:
                pending.append(batch)
                pending_rows += batch.num_rows
                progress.tick(rows + pending_rows)
                if pending_rows >= ROW_GROUP_ROWS:
                    writer.write_table(pa.Table.from_batches(pending))
                    rows += pending_rows
                    pending, pending_rows = [], 0
            if pending:
                writer.write_table(pa.Table.from_batches(pending))
                rows += pending_rows
            progress.done()
        writer.close()
        writer = None
        if rows == 0:
            raise ConvertError(f"{member.name!r}: header only, no data rows")
        os.replace(temp_path, member.out_path)
        return rows, repair.repairs, ''
    except (pa.ArrowInvalid, pa.ArrowException, UnicodeDecodeError, ValueError) as error:
        if writer is not None:
            writer.close()
            writer = None
        if os.path.exists(temp_path):
            os.remove(temp_path)
        finding = located(archive, member, encoding, delimiter, undoubled_quotes)
        if finding.truncated_tail and not strict and not limit:
            return retry_without_tail(archive, member, level, encoding, delimiter, infer_types,
                                      undoubled_quotes, finding)
        raise refusal(member, error, finding) from error
    finally:
        if writer is not None:
            writer.close()
        if os.path.exists(temp_path):
            os.remove(temp_path)


def retry_without_tail(archive, member, level, encoding, delimiter, infer_types, undoubled_quotes,
                       finding: Finding) -> tuple:
    """Convert again with the stream cut where the fragment begins."""
    rows, repairs, _ = convert_member(archive, member, level, encoding, delimiter, infer_types,
                                      undoubled_quotes, strict=True, limit=finding.cut_at)
    return rows, repairs, 'the truncated final record was dropped (a download that stopped mid-record)'


def convert_archive(archive_path: str, out_dir: str, level: int = COMPRESSION_LEVEL, encoding: str = 'utf8',
                    delimiter: str = ',', infer_types: bool = False, overwrite: bool = False,
                    undoubled_quotes: bool = False, strict: bool = False, report=None) -> list:
    """Convert every csv member; return the Parquet paths written, in
    archive order. `report(message)` receives progress lines."""
    say = report or (lambda message: None)
    if not zipfile.is_zipfile(archive_path):
        raise ConvertError(f"{archive_path!r} is not a zip archive")
    os.makedirs(out_dir, exist_ok=True)
    members, ignored = plan(archive_path, out_dir)
    for name in ignored:
        say(f"ignored    {name!r} (not a csv)")
    if not members:
        raise ConvertError(f"{os.path.basename(archive_path)!r} holds no csv member")
    written = []
    with zipfile.ZipFile(archive_path) as archive:
        for member in members:
            if os.path.exists(member.out_path) and not overwrite:
                raise ConvertError(f"{member.out_path!r} exists; use --overwrite to replace it")
            rows, repairs, note = convert_member(archive, member, level, encoding, delimiter, infer_types,
                                                 undoubled_quotes, strict)
            out_size = os.path.getsize(member.out_path)
            repaired = f", {repairs:,} inner quote(s) doubled" if repairs else ''
            say(f"wrote      {os.path.basename(member.out_path)!r}: {rows:,} rows, "
                f"{member.size / 1e6:,.1f} MB csv -> {out_size / 1e6:,.1f} MB parquet{repaired}")
            if note:
                say(f"note       {os.path.basename(member.out_path)!r}: {note}")
            written.append(member.out_path)
    return written


# End of file #
