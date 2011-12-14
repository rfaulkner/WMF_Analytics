
"""
    DJANGO VIEW DEFINITIONS:
    ========================
    
    Defines a view that produces json_objects embedded in callback function calls for external use
    
"""

__author__ = "Ryan Faulkner"
__revision__ = "$Revision$"
__date__ = "December 14th, 2011"

""" Import django modules """
from django.shortcuts import render_to_response
from django.template import RequestContext


""" Import python base modules """
import logging, sys, MySQLdb

""" Import Analytics modules """
import classes.DataLoader as DL

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


"""
    Produces a JSON object for a given campaign containing click rates for each banner running on that campaign
    
    The first query gets all of the banners and correspoding views on the corresponding campaign.  The second query gets impression counts for each banner
      
"""
def json_out(request, utm_campaign):
    
    utm_campaign = MySQLdb._mysql.escape_string(str(utm_campaign))
                                 
    dl = DL.DataLoader(db='db1025')
    lptl = DL.LandingPageTableLoader(db='db1008')
        
    start_time = lptl.get_earliest_campaign_view(utm_campaign)
    end_time = lptl.get_latest_campaign_view(utm_campaign)

    """ 
        Get the views from the given campaign for each banner 
        =====================================================
    """    
    
    logging.info('Determining views for campaign %s' % utm_campaign)
    sql = "select utm_source, count(*) as views from landing_page_requests where utm_campaign = '%s' and request_time >= %s and request_time <= %s group by 1" % (utm_campaign, start_time, end_time)
    results = dl.execute_SQL(str(sql))
    
    """ builf the condition string for banners to be used in SQL to retrieve impressions"""
    views = dict()
    banner_str = ''
    for row in results: 
        views[str(row[0])] = int(row[1])
        banner_str_piece = "utm_source = '%s' or " % row[0]
        banner_str = banner_str + banner_str_piece
    banner_str = banner_str[:-4]



    """ 
        Get the impressions from the given campaign for each banner 
        ===========================================================
    """        
    
    logging.info('Determining impressions for campaign %s' % utm_campaign)    
    sql = "select utm_source, sum(counts) from banner_impressions where (%s) and on_minute >= '%s' and on_minute <= '%s' group by 1" % (banner_str, start_time, end_time)    
    results = dl.execute_SQL(str(sql))
    
    """ Build JSON, compute click rates """
    click_rate = dict()
    json = 'insertStatistics({ '
    err_str = ''
    for row in results: 
        try:
            utm_source = row[0]
            click_rate = float(views[utm_source]) / float(int(row[1]))
            item = '"%s" : %s , ' % (utm_source, click_rate)
            json = json + item 
        
        except:
            err_str = err_str + utm_source + ' '
    
    json = json[:-2] + '});'
        
    return render_to_response('live_results/json_out.html', {'html' : json}, context_instance=RequestContext(request))

