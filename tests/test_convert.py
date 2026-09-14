"""
zipcsv2parquet: every archive shape that matters, and the contract.

tests/test_convert.py

One csv, two, none; a README and Finder junk beside the data; a BOM; a
quoted newline; a member in a folder; two members with the same
basename (refused); a member that does not parse (refused, nothing
left behind); header only (refused); an existing output (refused
without --overwrite); a large member streamed in several row groups;
and the CLI contract - paths on stdout only, everything else on
stderr, exit 1 when nothing was written.

Run: PYTHONPATH=src python3 tests/test_convert.py
"""

import io
import os
import sys
import shutil
import zipfile
import tempfile
import subprocess

import pyarrow.parquet as pq

from zipcsv2parquet.convert import ConvertError, convert_archive


WORK = tempfile.mkdtemp(prefix='zipcsv2parquet_')


def make_zip(name: str, members: dict) -> str:
    path = os.path.join(WORK, name)
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive:
        for member_name, content in members.items():
            archive.writestr(member_name, content)
    return path


def rows_of(path: str) -> list:
    return pq.read_table(path).to_pylist()


CSV = 'Package Number,Net Weight,Notes\n1000001,12.5,plain\n1000002,,"multi\nline"\n'


def test_one_csv_with_noise_beside_it() -> bool:
    print('\nOne csv beside a README and Finder junk: one Parquet, named for the member, all text, quoted newline kept...')
    archive = make_zip('one.zip', {'260913_IMS_Naknek.csv': '\ufeff' + CSV, 'README.txt': 'hi',
                                   '__MACOSX/._260913_IMS_Naknek.csv': 'junk', '.DS_Store': 'x'})
    out = os.path.join(WORK, 'one_out')
    written = convert_archive(archive, out)
    ok_name = [os.path.basename(path) for path in written] == ['260913_IMS_Naknek.parquet']
    table = pq.read_table(written[0])
    all_text = all(str(field.type) == 'string' for field in table.schema)
    bom_gone = table.schema.names[0] == 'Package Number'
    data = table.to_pylist()
    good = ok_name and all_text and bom_gone and data[1]['Net Weight'] is None and data[1]['Notes'] == 'multi\nline'
    print(f"  written={[os.path.basename(p) for p in written]} all text={all_text} BOM gone={bom_gone} "
          f"empty->null={data[1]['Net Weight'] is None} newline kept={data[1]['Notes'] == 'multi' + chr(10) + 'line'} -> {'OK' if good else 'FAIL'}")
    return good


def test_two_csvs_and_a_folder() -> bool:
    print('\nTwo csv members, one in a folder: two Parquets, in archive order, named for the members...')
    archive = make_zip('two.zip', {'a.csv': CSV, 'sub/b.csv': CSV})
    written = convert_archive(archive, os.path.join(WORK, 'two_out'))
    names = [os.path.basename(path) for path in written]
    good = names == ['a.parquet', 'b.parquet'] and all(len(rows_of(p)) == 2 for p in written)
    print(f"  {names} -> {'OK' if good else 'FAIL'}")
    return good


def test_no_csv_is_an_error() -> bool:
    print('\nAn archive with no csv member is refused...')
    archive = make_zip('none.zip', {'README.txt': 'hi', 'data.json': '{}'})
    try:
        convert_archive(archive, os.path.join(WORK, 'none_out'))
        print('  FAIL: converted nothing without complaint')
        return False
    except ConvertError as error:
        good = 'no csv member' in str(error)
        print(f"  {str(error)!r} -> {'OK' if good else 'FAIL'}")
        return good


def test_same_basename_twice_is_refused() -> bool:
    print('\nTwo members that would produce the same Parquet name are refused before anything is written...')
    archive = make_zip('dup.zip', {'x/data.csv': CSV, 'y/data.csv': CSV})
    out = os.path.join(WORK, 'dup_out')
    try:
        convert_archive(archive, out)
        print('  FAIL: wrote something')
        return False
    except ConvertError as error:
        nothing_written = not os.path.exists(out) or not os.listdir(out)
        good = 'would both become' in str(error) and nothing_written
        print(f"  refused={('would both become' in str(error))} nothing written={nothing_written} -> {'OK' if good else 'FAIL'}")
        return good


def test_malformed_member_is_refused_and_leaves_nothing() -> bool:
    print('\nA csv that does not parse (a row with too many fields) is refused and no partial file is left...')
    bad = 'A,B,C\n1,2,3\n' + ('4,5,6\n' * 5000) + '7,8,9,10,11\n' + ('1,2,3\n' * 5000)
    archive = make_zip('bad.zip', {'good.csv': CSV, 'bad.csv': bad})
    out = os.path.join(WORK, 'bad_out')
    try:
        convert_archive(archive, out)
        print('  FAIL: accepted the bad member')
        return False
    except ConvertError as error:
        leftovers = sorted(name for name in os.listdir(out) if 'bad' in name)
        good = 'does not parse cleanly' in str(error) and not leftovers and os.path.exists(os.path.join(out, 'good.parquet'))
        print(f"  refused={('does not parse cleanly' in str(error))} bad leftovers={leftovers or 'none'} "
              f"good member still written={os.path.exists(os.path.join(out, 'good.parquet'))} -> {'OK' if good else 'FAIL'}")
        return good


def test_header_only_is_refused() -> bool:
    print('\nA header-only csv is refused...')
    archive = make_zip('empty.zip', {'empty.csv': 'A,B,C\n'})
    try:
        convert_archive(archive, os.path.join(WORK, 'empty_out'))
        print('  FAIL: wrote an empty Parquet')
        return False
    except ConvertError as error:
        good = 'header only' in str(error)
        print(f"  {str(error)!r} -> {'OK' if good else 'FAIL'}")
        return good


def test_existing_output_needs_overwrite() -> bool:
    print('\nAn existing Parquet is not replaced unless overwrite is asked for...')
    archive = make_zip('again.zip', {'again.csv': CSV})
    out = os.path.join(WORK, 'again_out')
    convert_archive(archive, out)
    try:
        convert_archive(archive, out)
        refused = False
    except ConvertError as error:
        refused = 'exists' in str(error)
    replaced = bool(convert_archive(archive, out, overwrite=True))
    good = refused and replaced
    print(f"  refused without flag={refused} replaced with flag={replaced} -> {'OK' if good else 'FAIL'}")
    return good


def test_large_member_streams_in_row_groups() -> bool:
    print('\nA large member is streamed and lands in several row groups with the right row count...')
    rows = 600_000
    body = io.StringIO()
    body.write('Package Number,Product ID,Plant\n')
    for index in range(rows):
        body.write(f"P{1000000 + index},{12000 + index % 7},14 - SBS Naknek\n")
    archive = make_zip('big.zip', {'big.csv': body.getvalue()})
    written = convert_archive(archive, os.path.join(WORK, 'big_out'))
    metadata = pq.read_metadata(written[0])
    good = metadata.num_rows == rows and metadata.num_row_groups >= 2
    print(f"  rows={metadata.num_rows:,} row groups={metadata.num_row_groups} "
          f"csv {os.path.getsize(archive) / 1e6:.2f} MB zip -> {os.path.getsize(written[0]) / 1e6:.2f} MB parquet -> {'OK' if good else 'FAIL'}")
    return good


def test_infer_types_is_opt_in() -> bool:
    print('\n--infer-types lets pyarrow type the columns; without it everything is text...')
    archive = make_zip('types.zip', {'types.csv': 'Id,Weight\n1,2.5\n2,3.5\n'})
    text = convert_archive(archive, os.path.join(WORK, 'types_text'))
    typed = convert_archive(archive, os.path.join(WORK, 'types_typed'), infer_types=True)
    text_types = [str(f.type) for f in pq.read_schema(text[0])]
    typed_types = [str(f.type) for f in pq.read_schema(typed[0])]
    good = text_types == ['string', 'string'] and typed_types[0].startswith('int') and typed_types[1] == 'double'
    print(f"  text={text_types} inferred={typed_types} -> {'OK' if good else 'FAIL'}")
    return good


def test_cli_contract() -> bool:
    print('\nCLI: paths on stdout only, narrative on stderr, exit 1 when an archive yields nothing...')
    good_zip = make_zip('cli_good.zip', {'cli.csv': CSV, 'README.txt': 'x'})
    bad_zip = make_zip('cli_bad.zip', {'README.txt': 'x'})
    env = dict(os.environ, PYTHONPATH=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))
    out = os.path.join(WORK, 'cli_out')
    ok = subprocess.run([sys.executable, '-m', 'zipcsv2parquet', good_zip, '--out-dir', out],
                        capture_output=True, text=True, env=env)
    bad = subprocess.run([sys.executable, '-m', 'zipcsv2parquet', bad_zip, '--out-dir', out],
                         capture_output=True, text=True, env=env)
    stdout_is_paths = ok.stdout.strip() == os.path.join(out, 'cli.parquet')
    narrative_on_stderr = 'ignored' in ok.stderr and 'wrote' in ok.stderr and 'ignored' not in ok.stdout
    good = ok.returncode == 0 and stdout_is_paths and narrative_on_stderr and bad.returncode == 1 and bad.stdout == ''
    print(f"  good: exit={ok.returncode} stdout is the path={stdout_is_paths} narrative on stderr={narrative_on_stderr}; "
          f"bad: exit={bad.returncode} stdout empty={bad.stdout == ''} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [test_one_csv_with_noise_beside_it, test_two_csvs_and_a_folder, test_no_csv_is_an_error,
             test_same_basename_twice_is_refused, test_malformed_member_is_refused_and_leaves_nothing,
             test_header_only_is_refused, test_existing_output_needs_overwrite,
             test_large_member_streams_in_row_groups, test_infer_types_is_opt_in, test_cli_contract]
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
