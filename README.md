# ZipCSV-to-Parquet

Every csv inside a zip becomes a zstd-compressed Parquet file, without
extracting anything. One Parquet per csv member, named for the member,
written beside the archive.

    zipcsv2parquet export_2026-09-13.zip
    zipcsv2parquet *.zip --out-dir "$TMPDIR/parquet"

## Why it exists

A large csv from a database export can zip 60:1. Expanding a 600 MB
file just to read it costs 600 MB of disk, and if the folder is synced,
600 MB of upload. This reads each
csv member as a stream straight out of the archive and writes Parquet
(a few percent of the csv, and smaller than the zip at zstd level 15)
in row groups, so the csv never exists on disk at all.

The archive's contents are unpredictable - a README beside the data,
Finder's `__MACOSX` folder, two csvs, none - and the rules for that live
here, in one tool with a name, so that whatever reads the Parquet
afterwards (a data pipeline, a query engine, a spreadsheet converter)
can be simple.

## What it does with the contents

- Every member ending in `.csv` is data. Any other member is listed on
  stderr and ignored; Finder's leavings (`__MACOSX/`, `._` resource
  forks, `.DS_Store`, `Thumbs.db`) are skipped without a word.
- Each csv becomes `<member stem>.parquet`. Two members that would
  produce the same name are refused before anything is written; the
  archive's layout has to decide that, not the tool.
- Columns are TEXT. Package numbers, lot numbers and sequence numbers
  look numeric and are not; a downstream recipe types what it needs.
  `--infer-types` opts in to pyarrow's inference.
- An empty field is null. A quoted newline inside a field is kept. A
  UTF-8 BOM is fine. `--encoding` and `--delimiter` override the
  defaults (utf8, comma); nothing is sniffed.
- ONE repair is made, because it is a property of complete files, not
  broken ones: an inner quote the exporter forgot to double (a free-text
  field holding `40" high`) is doubled on the way through. Without that,
  a strict parser closes the field at the quote and every line break
  inside the field becomes a record boundary ("Expected 39 columns, got
  14"). The count is reported. The repair is a two-state machine over
  the quote characters, streamed so the file never has to be in memory.
- A csv that still does not parse cleanly - a row with the wrong number
  of fields, bad bytes - is refused with the reason, and no partial
  file is left behind. An export from a system should be complete; a
  tool that quietly salvaged a truncated one would hide that it was.
- A header-only csv is refused. An existing Parquet is not replaced
  unless `--overwrite`.

## The contract for scripts

Paths written go to STDOUT, one per line, nothing else. Progress, what
was ignored and errors go to STDERR. Exit 0 when every archive produced
at least one file, 1 otherwise. So:

    zipcsv2parquet "$zip" --out-dir "$tmp" | while read -r parquet; do
        erp parquet_to_xlsx.yaml --var source="$parquet" ...
    done

## Layout

    src/zipcsv2parquet/     the package (src layout)
      __main__.py           the command line
      convert.py            the conversion
      convert_rgx.py        the patterns (kept apart from the code)
      inner_quotes.py       the streaming repair of undoubled inner quotes
      _version.py           the version, date-based, bumped by script
    tests/test_convert.py   the battery
    dev_notes/              design decisions, and bump_version.py

## Install

    pip install -e .        # gives the `zipcsv2parquet` command

or run in place:

    PYTHONPATH=src python3 -m zipcsv2parquet FILE.zip

Version by script: `python3 dev_notes/bump_version.py`.

## Tests

    PYTHONPATH=src python3 tests/test_convert.py

Twelve shapes: one csv with noise beside it, two csvs (one in a folder),
none, duplicate basenames, a malformed member, header only, an existing
output, a 600,000-row member streamed in row groups, type inference as
opt-in, the stdout / stderr / exit-code contract, the undoubled
inner-quote shape (value intact, repair counted), and Finder junk kept
silent.
