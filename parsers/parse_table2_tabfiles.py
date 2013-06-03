#! /usr/bin/env python
__author__ = 'Andrew Boughton and the A2 Hack for Change team'
# Parse CDC MMWR data- the parser in this file is currently aimed at the format and contents of "table 2"
# as it is published weekly, and has only been tested on data 2006-2013.
# Results not guaranteed for other tables. Assumes datafiles in a subfolder of script directory named "tabdatafiles/"

import re
# JSON output of files seems stymied by the presence of strange characters that python json module can't parse
# These are probably the symbols used in the publication to indicate footnotes


class TabFileParser(object):
    def __init__(self, filename):
        """Opens and reads in a file.
        Then parses the sections of the file to yield a final dictionary of all the parsed data in the file
        (see usage example at end of script)
        Each object instance represents the parsed data for exactly one file, and also makes raw data available
         for examination via attributes
            .data_dict, .header_row, .column_names, .footnotes
        """

        # TODO: Pathname is manually specified, which is probably bad.
        with open("tabdatafiles/" + filename, 'rU') as f:
            fdata = f.read().splitlines()

        # Occasionally I'll reference seemingly weirdly chosen rows of the file- this indicates where the CDC
        # tab-separated file format consistently has useless rows (junk data like the word "test",
        # generic section descriptions, etc). We want to skip those.
        self.header_row = fdata[1]

        # Switch data processing mode when new block of file encountered. We can't use separate for loops to pick
        # out every section, because we'd prefer to resume processing the file where we left off (not from the start).

        # So to keep track of progress through the file,
        #  we use a generator expression to keep track of where we are in the list of rows, and specify
        # manually when we want the next line.
        # First 3 rows of datafile are blank + date header + blank. No point looping through them when we parse.
        datalines = (d for d in fdata[3:])

        # Now read through the distinct "sections" of the file. When we hit a blank line, while+break will
        # stop collecting lines for the given section and move on to the next.

        # TODO: Making the crawler more generic/subclassable by implementing get_sections functions for this stuff
        self.column_names = []
        while True:
            info = datalines.next()
            if info:
                self.column_names.append(info)
            else:
                break

        self.datarows = []
        while True:
            info = datalines.next()
            if info:
                self.datarows.append(info)
            else:
                break

        self.footnotes = []
        while True:
            info = datalines.next()
            if info:
                self.footnotes.append(info)
            else:
                break

        # Data section begins with notification that data is beginning, and the word "test". Column section begins with
        #  statement that column section is beginning. In both cases, we only want to pass data to the data parser.
        self.parse_datasection(self.datarows[2:],
                               self.column_names[1:],
                               self.footnotes)

    def get_date(self, header):
        """
         Gets the date the file was uploaded, based on regex. The date is usually in the first non-blank line of
         the file, so this function won't do anything especially exciting if given any other string of text
        :param header:
        """
        date_string = re.findall(
            r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d+,\s\d+',
            header)[0]
        # Had trouble capturing the data fields directly from first regex- got parenthesis errors. So, just use
        #  a second regex.

        # Interesting, the second regex will probably capture most of the dates in the files by itself. But it's
        #  probably worth the extra effort of the date-specific regex to be sure.
        date_tuple = re.findall(r'(\w+)\s(\d+),\s(\d+)', date_string)[0]
        return date_tuple

    def parse_column_headers(self):
        """
        Return dict with metadata on disease name, duration, and other relevant info

        The original idea was to do this manually, but then I realized (using head -2 *table2*) that the field names
           and file contents were laid out very very similarly in every file. So someone can generate a lookup
           table that manually replaces each instance of "disease cumulative 52 weeks median" with separately assigned
            metadata for that column. (disease, cumulative, 52 weeks, median)

        It would mean evaluating maybe 10-20 files with a few headings each- would be faster and way more reliable than
        trying to pick out the disease names and field data manually.
        """

        # First step: define variablity in column headers to understand field types
        # Second step: define list of known/accepted disease names
        # TODO: Never got around to implementing this. See notes for a better way.
        # Can use data from http://wwwn.cdc.gov/nndss/script/conditionlist.aspx?type=0&yr=2013
        # And hey, disease lists for previous years can be viewed by changing year URL parameter!!
        return

    def parse_datasection(self, datachunk, columnheaders, footnote_dict):
        """
        Produce nested dictionary with parsed data, ie d[columnname][rowname] for a single datapoint
        It's probably be better to set the class attribute variable based on the return value of the function,
        but I was running low on steam by this point.
        """
        # Datachunk and columnheaders should only be actual info- truncate and discard junk rows before passing in

        # Create data dictionary with keys = columnheaders
        self.data_dict = {k:{} for k in columnheaders}

        # Also store
        self.data_dict['date'] = self.get_date(self.header_row)

        for l in datachunk:
            data = l.split('\t')
            # Now map the data in the line to the data dictionary. Need to get column headers lines up with rows,
            # using a smidge of trickery since the data dictionary is a hash table
            # (doesn't preserve order of key addition- we want to associate the correct
            # part of the row with the right column name)

            for i, dp in enumerate(columnheaders):
                # There are a bunch of blank columns at the end of every row for some reason
                self.data_dict[dp][data[0]] = data[i]


    def parse_footnotes(self, footnotechunk):
        """Creates a dictionary to replace footnote codes with footnote text when seen later"""
        # Separates (code: desc info) based on the : character
        # TODO: Not used. For now we just include the footnote codes in the output data
        #   and trust the time series creator to deal with non-numeric/missing data manually
        #   But this could be used to improve parsing in the future, like if N meant something
        #    different across different data files.
        line_tuples = [l.lower().strip('.').split(': ')
                       for l in footnotechunk.splitlines()]
        return dict(line_tuples)


def get_file_list():
    """
    Oftentimes- such as when studying one specific disease- it'd be a waste of time to parse files that
    don't mention the disease.
    Find the filenames of interest- datasets specific to a given disease
    """
    # TODO: This would run command but no capture the output; figure out getting stdout later.
    # subprocess.Popen('grep -il syphilis tabdatafiles/*', shell=True)

    # Instead use manually generated list by doing that grep at command line. Hardcoding filenames
    # does probably save performance anyway when you do repeat runs. Assumes filelist in same folder as script.
    fhandle = open('syphilis_datafilenames.txt', 'rU')

    return fhandle.read().splitlines()

month_lookup = {'January':0,'February':1, 'March':2, 'April':3, 'May':4,
                'June':5,'July':6, 'August':7,'September':8,'October':9,
                'November':10, 'December':11}


if __name__ == "__main__":
    # For each file read, creates a python object with many attributes (raw and parsed data). Output is a list of objects.
    parsed_data = [TabFileParser(f) for f in get_file_list()]

    # Query each datafile using a nested dictionary: for each file (represented by a TabFileParser object instance),
    # the user can find out syphilis instances that week using <week_object>.data_dict['Column name']['location']

    # To get a time series, we loop over the data for all weeks. A list comprehension is a concise way to do that.

    # Sample use case: all the syphilis cases in the dataset, sorted in (date, reported count) pairs, for a given locale
    # Note that date is itself a tuple of (month, day, year)

    # Also note that State names in the files obey wildly, insanely inconsistent abbreviations,
    # and column/field names should be manually looked up.
    # Syphilis data is always in table 2H until 2008-9 and 2J thereafter
    visit_scenic_oregon = [(week_data.data_dict['date'],
                            week_data.data_dict['Syphillis, primary & secondary current week']['Oreg.'])
                            for week_data in parsed_data]
    # Above example only works because our dataset here is limited to files mentioning syphilis.
    #  If we had other files in the mix, those would yield a KeyError when given the disease name