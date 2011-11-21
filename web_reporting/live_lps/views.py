
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
import sys, datetime, logging, MySQLdb

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

    """ Process POST """     
    start_time, end_time, min_donation, view_order = process_post_vars(request)
    logging.debug('Finding live landing pages from %s to %s.' % (start_time, end_time))
    
    live_lps, columns = DL.CampaignReportingLoader(query_type='').query_live_landing_pages(start_time, end_time, min_donation=min_donation, view_order=view_order)
    
    if len(live_lps) == 0:
        html_table = '<br><p color="red"><b>No landing page data found.<b></p><br>'
    else:
        html_table = DR.DataReporting()._write_html_table(live_lps, columns)
    
    return render_to_response('live_lps/index.html', {'html_table' : html_table}, context_instance=RequestContext(request))


""" 
    Process POST for the index view
"""
def process_post_vars(request):
    
    end_time, start_time = TP.timestamps_for_interval(datetime.datetime.utcnow(), 1, hours=-24)
    
    # POST: minimum donations for records
    try:
        min_donations_var = MySQLdb._mysql.escape_string(request.POST['min_donations'].strip())
        min_donations_var = int(min_donations_var)
        
    except:
        min_donations_var = 0
    
    # POST Start Timestamp for records
    try:
        
        earliest_utc_ts_var = MySQLdb._mysql.escape_string(request.POST['utc_ts'].strip())
        
        """ If the user timestamp is earlier than the default start time run the query for the earlier start time  """
        ts_format = TP.getTimestampFormat(earliest_utc_ts_var)
    
        """ Ensure the validity of the timestamp input """
        if ts_format == TP.TS_FORMAT_FORMAT1:
            start_time = TP.timestamp_convert_format(earliest_utc_ts_var, TP.TS_FORMAT_FORMAT1, TP.TS_FORMAT_FLAT)
        elif ts_format == TP.TS_FORMAT_FLAT:
            start_time = earliest_utc_ts_var
                
    except Exception: # In the case the form was incorrectly formatted notify the user        
        pass
    
    # POST: minimum donations for records
    try:
        view_order = MySQLdb._mysql.escape_string(request.POST['view_order'].strip())
        
        if cmp(view_order, 'campaign') == 0:
            view_order_str = 'order by utm_campaign, country, language, landing_page desc'
        elif cmp(view_order, 'country') == 0:
            view_order_str = 'order by country, language, utm_campaign, landing_page desc'
        
    except:
        view_order_str = 'order by utm_campaign, country, language, landing_page desc'
    
    return start_time, end_time, min_donations_var, view_order_str
