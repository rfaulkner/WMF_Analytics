
"""
    DJANGO VIEW DEFINITIONS:
    ========================
    
    Defines the views for campaigns application.  This is meant to serve as a view into currently running campaigns and 
    generating tests for them.

    Views:
    
    index             -- the index page shows a listing of campaigns and the donations they have received along with a link to a generated report if exists 
    show_campaigns    -- this view shows the view stats for a campaign and allows the user to execute a test and generate a report
    show_report       -- this view simply enables linking to existing reports
    
"""

__author__ = "Ryan Faulkner"
__revision__ = "$Revision$"
__date__ = "June 20th, 2011"


""" Import django modules """
from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponse 

""" Import python base modules """
import sys, datetime, operator, MySQLdb, logging

""" Import Analytics modules """
import classes.DataReporting as DR
import classes.DataLoader as DL
import classes.FundraiserDataHandler as FDH
import classes.TimestampProcessor as TP
import config.settings as projSet


""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


"""
    CAMPAIGNS INDEX VIEW - historic list of campaigns run
    
    Index page for finding the latest camapigns.  Displays a list of recent campaigns with more than k donations over the last n hours. 
    Also if a report exists a link is provided.
    
"""
def index(request, **kwargs):
    
    crl = DL.CampaignReportingLoader('totals')
    filter_data = True
    
    """ 
        PROCESS POST KWARGS 
        ===================
    """
    err_msg = ''
    try:
        err_msg = str(kwargs['kwargs']['err_msg'])
    except:
        pass
    
    """ 
        PROCESS POST VARS 
        =================
        
        Filter out rows based on POST vars
        
    """
    
    """ Process error message """
    try:
        err_msg = MySQLdb._mysql.escape_string(request.POST['err_msg'])
    except KeyError:
        pass

    """ Process data filtering vars  """
    try:
        min_donations_var = MySQLdb._mysql.escape_string(request.POST['min_donations'])
        earliest_utc_ts_var = MySQLdb._mysql.escape_string(request.POST['utc_ts'])
        
        if len(earliest_utc_ts_var) == 0:
            earliest_utc_ts_var = '0'
            
    except KeyError:
        
        min_donations_var = ''
        earliest_utc_ts_var = ''
        filter_data = False
    
    """ Look for campaigns over the last 7 days """ 
    start_time_obj =  datetime.datetime.now() + datetime.timedelta(days=-7)
    end_time = TP.timestamp_from_obj(datetime.datetime.now() + datetime.timedelta(hours=8),1,3)    
    start_time = TP.timestamp_from_obj(start_time_obj,1,3)
    
    """ If the user timestamp is earlier than the default start time run the query for the earlier start time  """
    ts_format = TP.getTimestampFormat(earliest_utc_ts_var)
    
    if ts_format > 0:
        earliest_utc_obj = TP.timestamp_to_obj(earliest_utc_ts_var, ts_format)
        
        if ts_format != 1:
            earliest_utc_ts_var = TP.timestamp_convert_format(earliest_utc_ts_var, ts_format, 1)
             
        if earliest_utc_obj < start_time_obj:
            start_time = earliest_utc_obj
        
    """ 
        GENERATE CAMPAIGN DATA 
        ======================
        
    """
    
    campaigns, all_data = crl.run_query({'metric_name':'earliest_timestamp','start_time':start_time,'end_time':end_time})
    
    """ Sort campaigns by earliest access """    
    sorted_campaigns = sorted(campaigns.iteritems(), key=operator.itemgetter(1))
    sorted_campaigns.reverse()
    
    """ 
        FILTER CAMPAIGN DATA
        ====================
        
    """
    if filter_data:
        try:
            
            if min_donations_var == '':
                min_donations = 0
            else:
                min_donations = int(min_donations_var)
            
            earliest_utc_ts = int(earliest_utc_ts_var)
        except:
            err_msg = 'Filter fields are incorrect.'

            filter_data = False
    else:
        min_donations_var = ''
        earliest_utc_ts_var = ''

    new_sorted_campaigns = list()
    for campaign in sorted_campaigns:
        key = campaign[0]
        
        if campaign[1] > 0:
            name = all_data[key][0]
            if name  == None:
                name = 'none'
            
            timestamp = TP.timestamp_convert_format(all_data[key][3], 1, 2)
            
            if filter_data: 
                if all_data[key][2] > min_donations and int(all_data[key][3]) > earliest_utc_ts:
                    new_sorted_campaigns.append([campaign[0], campaign[1], name, timestamp, all_data[key][2], all_data[key][4]])
            else:
                new_sorted_campaigns.append([campaign[0], campaign[1], name, timestamp, all_data[key][2], all_data[key][4]])
    
    sorted_campaigns = new_sorted_campaigns

    return render_to_response('campaigns/index.html', {'campaigns' : sorted_campaigns, 'err_msg' : err_msg, 'min_donations' : min_donations_var, 'earliest_utc' : earliest_utc_ts_var}, context_instance=RequestContext(request))

    


"""

    CAMPAIGNS DETAIL VIEW - View breakdown of campaigns and test generation form

    Shows view stats over the time range for which the campaign receives landing page hits.  A form is also generated from the associated template
    that allows a test report to be generated.

"""
def show_campaigns(request, utm_campaign, **kwargs):
    
    """ 
        PROCESS POST KWARGS 
        ===================
    """
    
    err_msg = ''
    try:
        err_msg = str(kwargs['kwargs']['err_msg'])
    except:
        pass
    
    test_type_override = ''
    try:
        test_type_override = MySQLdb._mysql.escape_string(request.POST['test_type_override'])
        
        if test_type_override == 'Banner':
            test_type_var = FDH._TESTTYPE_BANNER_
        elif test_type_override == 'Landing Page':
            test_type_var = FDH._TESTTYPE_LP_
        elif test_type_override == 'Banner and LP':
            test_type_var = FDH._TESTTYPE_BANNER_LP_
            
    except:
        test_type_var = ''
        pass
    
    
    try:
            
        """ Find the earliest and latest page views for a given campaign  """
        start_time = DL.LandingPageTableLoader().get_earliest_campaign_view(utm_campaign)
        end_time = DL.LandingPageTableLoader().get_latest_campaign_view(utm_campaign)

        interval = 1
            
        """ Create reporting object to retrieve campaign data and write plots to image respo on disk """
        ir = DR.IntervalReporting(was_run=False, use_labels=False, font_size=20, plot_type='line', query_type='campaign', file_path=projSet.__web_home__ + 'campaigns/static/images/')
        
        """ Produce analysis on the campaign view data """        
        ir.run(start_time, end_time, interval, 'views', utm_campaign, {})
                                 
        """ 
            ESTIMATE THE START AND END TIME OF THE CAMPAIGN
            ===============================================
            
            Search for the first instance when more than 10 views are observed over a sampling period
        """
        
        col_names = ir._data_loader_.get_column_names()
            
        views_index = col_names.index('views')
        ts_index = col_names.index('ts')
        
        row_list = list(ir._data_loader_._results_) # copy the query results
        for row in row_list:
            if row[views_index] > 100:
                start_time_est = row[ts_index]
                break
        row_list.reverse()
        for row in row_list:
            if row[views_index] > 100:
                end_time_est = row[ts_index]
                break
        
        
        """
            BUILD THE VISUALIZATION FOR THE TEST VIEWS OF THIS CAMAPAIGN
            ============================================================        
        """
        
        """ Read the test name """
        ttl = DL.TestTableLoader()
        row = ttl.get_test_row(utm_campaign)
        test_name = ttl.get_test_field(row ,'test_name')
        
        """ Regenerate the data using the estimated start and end times """
        ir = DR.IntervalReporting(was_run=False, use_labels=False, font_size=20, plot_type='line', query_type='campaign', file_path=projSet.__web_home__ + 'campaigns/static/images/')
        ir.run(start_time_est, end_time_est, interval, 'views', utm_campaign, {})
            
        """ Determine the type of test (if not overridden) and retrieve the artifacts  """
        test_type, artifact_name_list = FDH.get_test_type(utm_campaign, start_time, end_time, DL.CampaignReportingLoader(''), test_type_var)
        
        return render_to_response('campaigns/show_campaigns.html', {'utm_campaign' : utm_campaign, 'test_name' : test_name, 'start_time' : start_time_est, 'end_time' : end_time_est, 'artifacts' : artifact_name_list, 'test_type' : test_type, 'err_msg' : err_msg}, context_instance=RequestContext(request))    
    
    except Exception as inst:
            
        logging.error('Failed to correctly produce campaign diagnostics.')
        logging.error(type(inst))
        logging.error(inst.args)
        logging.error(inst)
    
        """ Return to the index page with an error """
        err_msg = 'There is insufficient data to analyze this campaign: %s.  Check to see if the <a href="/LML/">impressions have been loaded</a>. <br><br>ERROR:<br><br>%s' % (utm_campaign, inst.__str__())
        
        return index(request, kwargs={'err_msg' : err_msg})



"""
    
    Retrieve a report from the database and display

"""
def show_report(request, utm_campaign):
    
    ttl = DL.TestTableLoader()
    
    row = ttl.get_test_row(utm_campaign)
    
    try:
        html = row[7]
    except:
        html = '<br><br><center><p><b>Was unable to retrieve report</b></p><br><br><a href="/">Home</a></center>'
        
    return HttpResponse(html)

    
    