"""
Where exactly a csv member goes wrong, and a sign of life while it is read.

zipcsv2parquet/diagnose.py

pyarrow's parse error says "Expected 39 columns, got 14" and quotes the
row it assembled, but not WHERE - and the quoted row can begin at a line
that looks perfectly normal, so a person cannot find it either. When
the conversion fails, `locate_bad_record` re-reads the member (through
the same quote repair the conversion used) with Python's csv reader,
which counts physical lines, and reports the first record whose field
count differs from the header's: its record number, the physical lines
it spans, the record before it, and the raw bytes of those lines with
every quote and line break visible. That is what a person needs to see
to say what the exporter did.

`Progress` prints a line to stderr every couple of seconds while a
large member is read - rows so far and elapsed time - only when stderr
is a terminal, and clears it when done.
"""

import csv
import io
import sys
import time
import zipfile

from zipcsv2parquet.inner_quotes import InnerQuoteRepair


class Finding:
    """What the csv module found: a description for a person, and whether
    it is a truncated tail - the LAST record, short, with no final newline
    - which a caller may choose to drop (cut_at is the byte offset where
    that last physical line starts)."""

    def __init__(self, description: str, truncated_tail: bool = False, cut_at: int = 0):
        self.description = description
        self.truncated_tail = truncated_tail
        self.cut_at = cut_at


def locate_bad_record(archive: zipfile.ZipFile, member_name: str, encoding: str, delimiter: str,
                      undoubled_quotes: bool, context_lines: int = 2) -> Finding:
    """The first bad record, or a Finding with an empty description if the
    csv module finds nothing wrong (then the fault is pyarrow-specific)."""
    with archive.open(member_name) as stream:
        repaired = InnerQuoteRepair(stream, delimiter.encode('ascii'), undoubled_quotes)
        text = io.TextIOWrapper(io.BufferedReader(repaired), encoding=encoding.replace('utf8', 'utf-8-sig'),
                                newline='')
        reader = csv.reader(text, delimiter=delimiter)
        header = next(reader, None)
        if header is None:
            return Finding('')
        width = len(header)
        previous_end = reader.line_num
        previous_record = header
        for index, record in enumerate(reader, 1):
            start, end = previous_end + 1, reader.line_num
            if len(record) != width:
                is_last = next(reader, None) is None
                description = describe(archive, member_name, index, width, len(record), start, end,
                                       previous_record, record, context_lines)
                if is_last and len(record) < width:
                    tail = tail_facts(archive, member_name, start)
                    if tail is not None:
                        description += ("\n  This is the LAST record, short, and the file does not end with a newline: "
                                        "a download that stopped mid-record. Everything above it is complete "
                                        "(dropped and reported unless --strict).")
                        return Finding(description, truncated_tail=True, cut_at=tail)
                return Finding(description)
            previous_end, previous_record = end, record
    return Finding('')


def tail_facts(archive: zipfile.ZipFile, member_name: str, last_start_line: int):
    """The byte offset where physical line `last_start_line` begins, if
    the member does not end with a newline; else None."""
    offset = 0
    start_offset = None
    last = b''
    with archive.open(member_name) as stream:
        for number, raw in enumerate(stream, 1):
            if number == last_start_line:
                start_offset = offset
            offset += len(raw)
            last = raw
    if start_offset is None or last.endswith(b'\n') or last.endswith(b'\r'):
        return None
    return start_offset


def describe(archive, member_name, index, width, got, start, end, previous_record, record, context_lines) -> str:
    lines = [f"record {index:,} has {got} fields, the header has {width}; "
             f"it spans physical line{'s' if end > start else ''} {start:,}" + (f"-{end:,}" if end > start else '')]
    lines.append(f"  the record before it ends: ...{', '.join(previous_record[-3:])!r}"[:200])
    lines.append(f"  this record as parsed: {record[:6]!r}"[:300] + (' ...' if len(record) > 6 else ''))
    lines.append("  raw bytes of those physical lines (quotes and line breaks visible):")
    first = max(1, start - context_lines)
    last = end + context_lines
    with archive.open(member_name) as stream:
        for number, raw in enumerate(stream, 1):
            if number < first:
                continue
            if number > last:
                break
            marker = '>>' if start <= number <= end else '  '
            lines.append(f"  {marker} {number:>9,}: {raw[:240]!r}" + (' ...' if len(raw) > 240 else ''))
    return '\n'.join(lines)


class Progress:
    """A line on stderr every `every` seconds while work goes on; only
    when stderr is a terminal. `tick(rows)` is cheap to call often."""

    def __init__(self, label: str, every: float = 2.0):
        self.label = label
        self.every = every
        self.enabled = sys.stderr.isatty()
        self.started = time.time()
        self.last = self.started
        self.shown = False

    def tick(self, rows: int):
        if not self.enabled:
            return
        now = time.time()
        if now - self.last < self.every:
            return
        self.last = now
        elapsed = now - self.started
        sys.stderr.write(f"\r  reading   {self.label}: {rows:,} rows, {elapsed:.0f} s ...")
        sys.stderr.flush()
        self.shown = True

    def done(self):
        if self.shown:
            sys.stderr.write("\r" + " " * 79 + "\r")
            sys.stderr.flush()


# End of file #
