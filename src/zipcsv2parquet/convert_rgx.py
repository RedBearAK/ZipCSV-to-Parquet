"""
Patterns for deciding which archive members are csv data.

zipcsv2parquet/convert_rgx.py
"""

import re


# a data member: the name ends in .csv, any case
csv_member_rgx = re.compile(r'\.csv$', re.IGNORECASE)

# archive noise that is never data, whatever its extension: macOS resource
# forks and the __MACOSX folder Finder adds, dotfiles, Thumbs.db
junk_member_rgx = re.compile(r'(^|/)(__MACOSX/|\._|\.DS_Store|Thumbs\.db)', re.IGNORECASE)


# End of file #
