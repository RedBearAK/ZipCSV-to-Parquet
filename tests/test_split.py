"""
parquetsplit: exact cuts, every row in one part, in order; a small file
is not split; the contract for scripts.

tests/test_split.py

Run: PYTHONPATH=src python3 tests/test_split.py
"""

import os
import sys
import shutil
import tempfile
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq

from zipcsv2parquet.split import SplitError, split_parquet


WORK = tempfile.mkdtemp(prefix='parquetsplit_')


def make(name: str, rows: int, row_group: int = 100_000) -> str:
    path = os.path.join(WORK, name)
    table = pa.table({'Id': [f"P{i:08d}" for i in range(rows)], 'Weight': [str((i % 1000) / 10) for i in range(rows)]})
    pq.write_table(table, path, compression='zstd', compression_level=15, row_group_size=row_group)
    return path


def test_exact_cut_in_order() -> bool:
    print('\nA cut lands at exactly the requested row, parts are in order, and every row is in exactly one part...')
    source = make('big.parquet', 2_300_000)
    parts = split_parquet(source, WORK, 1_000_000)
    counts = [pq.read_metadata(p).num_rows for p in parts]
    first = pq.read_table(parts[1]).column('Id')[0].as_py()
    last = pq.read_table(parts[0]).column('Id')[-1].as_py()
    ids = []
    for part in parts:
        ids.extend(pq.read_table(part).column('Id').to_pylist())
    good = (counts == [1_000_000, 1_000_000, 300_000] and last == 'P00999999' and first == 'P01000000'
            and ids == [f"P{i:08d}" for i in range(2_300_000)])
    print(f"  parts={[os.path.basename(p) for p in parts]} counts={counts} seam={last}|{first} all rows once in order={ids == [f'P{i:08d}' for i in range(2_300_000)]} -> {'OK' if good else 'FAIL'}")
    return good


def test_cut_not_on_a_row_group_seam() -> bool:
    print('\nA cut inside a row group (rows 100,000 each, cut at 250,000) is still exact...')
    source = make('rg.parquet', 600_000)
    parts = split_parquet(source, WORK, 250_000)
    counts = [pq.read_metadata(p).num_rows for p in parts]
    seam = pq.read_table(parts[0]).column('Id')[-1].as_py()
    good = counts == [250_000, 250_000, 100_000] and seam == 'P00249999'
    print(f"  counts={counts} last of part 1={seam} -> {'OK' if good else 'FAIL'}")
    return good


def test_small_file_is_returned_unsplit() -> bool:
    print('\nA file that fits in one part is returned as itself and nothing is written...')
    source = make('small.parquet', 5000)
    before = sorted(os.listdir(WORK))
    parts = split_parquet(source, WORK)          # the default size, 250,000
    good = parts == [source] and sorted(os.listdir(WORK)) == before
    print(f"  returned itself={parts == [source]} nothing written={sorted(os.listdir(WORK)) == before} -> {'OK' if good else 'FAIL'}")
    return good


def test_existing_parts_need_overwrite() -> bool:
    print('\nExisting parts are not replaced without --overwrite...')
    source = make('again.parquet', 30_000)
    split_parquet(source, WORK, 10_000)
    try:
        split_parquet(source, WORK, 10_000)
        refused = False
    except SplitError as error:
        refused = 'exists' in str(error)
    replaced = len(split_parquet(source, WORK, 10_000, overwrite=True)) == 3
    good = refused and replaced
    print(f"  refused={refused} replaced with flag={replaced} -> {'OK' if good else 'FAIL'}")
    return good


def test_cli_contract() -> bool:
    print('\nCLI: part paths on stdout, narrative on stderr; a small file prints its own path...')
    source = make('cli.parquet', 25_000)
    env = dict(os.environ, PYTHONPATH=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))
    out = os.path.join(WORK, 'cli_out')
    result = subprocess.run([sys.executable, '-m', 'zipcsv2parquet.split', source, '--rows', '10000', '--out-dir', out],
                            capture_output=True, text=True, env=env)
    lines = result.stdout.split()
    small = subprocess.run([sys.executable, '-m', 'zipcsv2parquet.split', source, '--out-dir', out],
                           capture_output=True, text=True, env=env)
    good = (result.returncode == 0 and len(lines) == 3 and all(l.endswith('.parquet') for l in lines)
            and 'wrote' in result.stderr and small.stdout.strip() == source)
    print(f"  parts on stdout={len(lines)} narrative on stderr={'wrote' in result.stderr} small prints itself={small.stdout.strip() == source} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [test_exact_cut_in_order, test_cut_not_on_a_row_group_seam, test_small_file_is_returned_unsplit,
             test_existing_parts_need_overwrite, test_cli_contract]
    passed = 0
    try:
        for test in tests:
            if test():
                passed += 1
    finally:
        shutil.rmtree(WORK, ignore_errors=True)
    print(f"\n{passed}/{len(tests)} tests passed")
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    sys.exit(main())


# End of file #
