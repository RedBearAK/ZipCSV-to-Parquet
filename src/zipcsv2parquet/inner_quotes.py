"""
A file-like wrapper that doubles the inner quotes an export forgot to.

zipcsv2parquet/inner_quotes.py

Some exporters write a free-text value like  40" high  inside a quoted
field without doubling the quote. A strict csv parser reads that quote as CLOSING the
field, and from there every line break inside a note becomes a record
boundary - "Expected 39 columns, got 14" on a line that starts with a
workflow value. This is a property of complete files, not broken ones,
so it is repaired, not refused. The rule, as a stream so a 600 MB
member never has to be in memory:

- outside a field, a quote right after a delimiter or a line start
  OPENS a quoted field; any other quote outside a field is literal in
  an unquoted field and is left alone;
- inside a field, `""` is an escaped quote and is kept; a quote
  followed by a delimiter, a line end or EOF CLOSES the field; any
  other quote is an inner quote the export forgot to double, and is
  doubled here.

The machine needs one byte of lookahead, so the last byte of every
chunk is held back until the next chunk (or EOF) says what follows it.
Embedded line breaks inside quoted fields pass through untouched.

The `""` ambiguity (2026-09-14). A value that ENDS with a quote -
`Green Roe P-1F 3 x 7.5"` - is written by such an exporter as
`"...7.5""` followed by the delimiter. Standard csv reads `""` as an
escaped quote with the field still open, so the parser swallows the
rest of the record and part of the next until a lone quote appears:
"Expected 39 columns, got 14" on what is a complete row. The bytes
cannot say which reading is meant. What CAN say is a property of the
exporter: one that never doubles quotes cannot have written an escape,
so `undoubled_quotes=True` reads every quote inside a field that is not
followed by a delimiter, a line end or EOF as an inner quote - including
one followed by another quote - and doubles it. Off by default: the
standard reading stands for exporters that escape properly.
"""

import io

QUOTE = 0x22
DELIMITER = 0x2C
LINE_FEED = 0x0A
CARRIAGE_RETURN = 0x0D


class InnerQuoteRepair(io.RawIOBase):
    """Reads from `source` (any object with read(n) -> bytes) and yields
    the same bytes with inner quotes doubled. Counts the repairs."""

    def __init__(self, source, delimiter: bytes = b',', undoubled_quotes: bool = False):
        super().__init__()
        self.source = source
        self.delimiter = delimiter[0]
        self.undoubled_quotes = undoubled_quotes
        self.inside = False
        self.skip_next = False          # the second half of a "" pair
        self.previous = None            # last byte emitted or consumed
        self.held = b''                 # the lookahead byte, not yet decided
        self.exhausted = False
        self.repairs = 0
        self.out = bytearray()

    def readable(self) -> bool:
        return True

    def _process(self, data: bytes, final: bool):
        """Run the machine over data (with self.held prepended); keep the
        last byte back unless final."""
        data = self.held + data
        limit = len(data) if final else len(data) - 1
        if limit <= 0:
            self.held = data
            return
        out = self.out
        copied = 0
        for index in range(limit):
            byte = data[index]
            if byte != QUOTE:
                self.previous = byte
                continue
            if self.skip_next:
                self.skip_next = False
                self.previous = byte
                continue
            following = data[index + 1] if index + 1 < len(data) else None
            if self.inside:
                if following in (None, self.delimiter, LINE_FEED, CARRIAGE_RETURN):
                    self.inside = False
                elif following == QUOTE and not self.undoubled_quotes:
                    self.skip_next = True           # standard csv: an escaped quote
                else:
                    out += data[copied:index]
                    out += b'""'
                    copied = index + 1
                    self.repairs += 1
            elif self.previous in (None, self.delimiter, LINE_FEED, CARRIAGE_RETURN):
                self.inside = True
            self.previous = byte
        out += data[copied:limit]
        self.held = data[limit:]

    def readinto(self, buffer) -> int:
        wanted = len(buffer)
        while len(self.out) < wanted and not self.exhausted:
            chunk = self.source.read(max(wanted, 1 << 20))
            if not chunk:
                self.exhausted = True
                self._process(b'', final=True)
                break
            self._process(chunk, final=False)
        take = min(wanted, len(self.out))
        buffer[:take] = self.out[:take]
        del self.out[:take]
        return take


# End of file #
