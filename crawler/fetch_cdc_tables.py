
__author__ = 'Andrew Boughton and the A2 Hack for Change team'

import urllib2
from BeautifulSoup import BeautifulSoup


class CrawlTables(object):
    """Crawls morbidity table data from the Center for Disease Control's Morbidity table web service.
    http://wonder.cdc.gov/
    http://www.cdc.gov/mmwr/distrnds.html
    http://wonder.cdc.gov/mmwr/mmwrmorb.asp

    And compare the ugly tables put on the web/ via Tab delimited files to the API for AIDS data. Morbidity records
    could stand for a similar treatment!
    http://wonder.cdc.gov/aids-v2001.html
    """

    def __init__(self, startyear=2006, endyear=2013, startweek=1, endweek=12):
        """
        Set up and run the crawler whenever an instance of this MMWR crawler object is instantiated. See example at end
        of file for usage: time range (weeks and years) can be manually specified. There's no sanity checking and
        limited error handling, so try to pick values that make sense. :)
        """
        self.urls = []

        years = xrange(startyear, endyear + 1, 1)

        for year in years:
            # Figure out what the list of weeks in that year is (based on how many years we're crawling)
            if year != endyear:
                # Manually checked all years from 2006-2013 and found that only 2008 had 53 weeks
                lastweek = (52 if year != 2008 else 53)
                if year == startyear:
                    weeks = xrange(startweek, lastweek + 1)
                else:
                    weeks = xrange(1, lastweek + 1)
            else:
                if startyear == endyear:
                    weeks = xrange(startweek, endweek + 1)
                else:
                    weeks = xrange(1, endweek + 1)
            for w in weeks:
                tables = self.get_allowed_tables(year, w)
                if tables:
                    # If for some reason there are no tables returned- say if we request data for a week with none-
                    # don't do anything. Otherwise, fetch URL of page and save it.
                    # Default is to save to the same folder as this script; I moved them later via command line.
                    #  Messy, but efficient.
                    for t in tables:
                        fname = "{0}_wk{1:02}_table{2}.csv".format(year, w, t)
                        with open(fname, 'w') as f:
                            f.write(self.get_tabfile(year, w, t))

    def get_tabfile(self, year, week, tablename):
        """
        Fetches the contents (and returns a string) of a specific MWR tab-delimited file, using
         the manually specified URL pattern from the CDC site and the specified year/month/tablename.
         There's basically no error handling in the event of server issues (which will yield an HTTPError).
         Just try again later- the bulk downloader shouldn't need to be run often anyway.
        """
        # Can be used to get HTML files instead by replacing request=Export with request=Submit.

        file_url = 'http://wonder.cdc.gov/mmwr/mmwr_reps.asp?mmwr_year={0}&mmwr_week={1:02}&mmwr_table={2}&request=Export'.format(
            year, week, tablename)
        self.urls.append(file_url)

        datafile = urllib2.urlopen(file_url)
        return datafile.read()

    def get_allowed_tables(self, year, week):
        """
        The list of tables published may vary from week to week. This fetches the list from the CDC mmwr pages so
        that none are missed.
        """
        table_list_page = urllib2.urlopen(
            'http://wonder.cdc.gov/mmwr/mmwrmorb2.asp?mmwr_year={0}&mmwr_week={1:02}'.format(
                year, week))

        soup = BeautifulSoup(table_list_page.read())

        try:
            mmwr_table_tags = soup.find('select', {'name': 'mmwr_table'}).findAll('option')
        except AttributeError:
            # Most likely to encounter an attribute error if you request a year+week combo that doesn't exist.
            # (like week 55)
            return None

        mmwr_table_names = [e.attrs[0][1] for e in mmwr_table_tags]
        return mmwr_table_names


if __name__ == '__main__':
    ############
    # Sample use case below
    # skipped 2007 wk 13 because one of the tables that week generated errors. Seemingly on web site too?
    crawled = CrawlTables(startyear=1996, endyear=2005, startweek=1, endweek=52)

    # The line below can be uncommented to see/output list of all urls visited
    #print crawled.urls