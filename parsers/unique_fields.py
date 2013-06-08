__author__ = 'Andrew Boughton and the A2 Hack for Change team'

# Find the unique row and column names across all fields in a given file
from parse_table2_tabfiles import *

# Tables I, J, and K were added only in 2010
#filename_list = get_tables_timerange(startyear=2010,endyear=2013, startweek=1, endweek=21, tablename="2K")

# Had some issues with malformed tables from 1996-1999, so restrict it to
filename_list = get_filenames_in_directory('../tabdatafiles', pattern='2*_wk*_table2*.tab')
print len(filename_list)

# Load and parse all the files in question
if filename_list:
    parsed_data = [TabFileParser(f, filepath='../tabdatafiles') for f in filename_list]

# Store all unique row and column headings
col_headings = set()
row_headings = set()

for f in parsed_data:
    these_cn = f.table_data.keys()
    these_rn = f.table_data[these_cn[0]].keys()

    col_headings.update(set(these_cn))
    row_headings.update(set(these_rn))

import pprint
print len(col_headings), "col headings"
pprint.pprint(col_headings)

print ''
print len(row_headings), 'row headings'
pprint.pprint(row_headings)
