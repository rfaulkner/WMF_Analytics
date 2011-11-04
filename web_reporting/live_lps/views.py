
"""
    DJANGO VIEW DEFINITIONS:
    ========================
    
    Defines the views for ongoing donations.  This view interacts with templates serving embedded AJAX plotting to provide the user with a 
    a running feed of campaign, banner, and landing page performance based on donations received.

    Views:
    
    index             -- Queries the fundraiser database for donations and sends the data to the template to be displayed 
    
    Helpers:
    
    get_data_lists        -- composes the donation query data into a format expected by the template
    combine_data_lists    -- combines the separate data sets from different queries into one in a format expected by the template 
    
"""

__author__ = "Ryan Faulkner"
__revision__ = "$Revision$"
__date__ = "August 31st, 2011"

""" Import django modules """
from django.shortcuts import render_to_response
from django.template import RequestContext


""" Import python base modules """
import sys, datetime, logging

""" Import Analytics modules """
import classes.TimestampProcessor as TP
import classes.DataReporting as DR
import classes.DataLoader as DL


""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

"""
    Index page for live landing page results. 
"""
def index(request):
    
    """  """
    end_time, start_time = TP.timestamps_for_interval(datetime.datetime.utcnow() + datetime.timedelta(hours=5), 1, hours=-24)
    logging.debug('Finding live landing pages from %s to %s.' % (start_time, end_time))
    #start_time = '20110812000000'
    #end_time = '20110813000000'
    
    live_lps = DL.CampaignReportingLoader('').query_live_landing_pages(start_time, end_time)
    
    if len(live_lps) == 0:
        html_table = '<br><p color="red"><b>No landing page data found.<b></p><br>'
    else:
        html_table = DR.DataReporting()._write_html_table(live_lps, ['Country', 'Language', 'Live Banners', 'Landing Page', 'Views', 'Donations', 'Total Amount ($)'])
    
    return render_to_response('live_lps/index.html', {'html_table' : html_table}, context_instance=RequestContext(request))

