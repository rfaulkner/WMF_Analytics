
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
    Index page for finding the latest camapigns.  Displays a list of recent campaigns with more than k donations over the last n hours. 
    Also if a report exists a link is provided.
    
"""
def index(request):
    
    err_msg = ''

    """ Parse the filter fields """
    filter_data = True

    try:
        min_donations_var = MySQLdb._mysql.escape_string(request.POST['min_donations'])
        earliest_utc_ts_var = MySQLdb._mysql.escape_string(request.POST['utc_ts'])
        
        if len(earliest_utc_ts_var) == 0:
            earliest_utc_ts_var = '0'
    except KeyError:
        
        filter_data = False
    
        
    """ Interface with the DataLoader """
    
    crl = DL.CampaignReportingLoader('totals')
    
    start_time_obj =  datetime.datetime.now() + datetime.timedelta(days=-21)
    end_time = TP.timestamp_from_obj(datetime.datetime.now() + datetime.timedelta(hours=8),1,3)    
    start_time = TP.timestamp_from_obj(start_time_obj,1,3)
    
    """ If the user timestamp is earlier than the default start time run the query for the earlier start time  """
    ts_format = TP.getTimestampFormat(earliest_utc_ts_var)
    if ts_format > 0:
        earliest_utc_obj = TP.timestamp_to_obj(earliest_utc_ts_var, ts_format)
        
        if earliest_utc_obj < start_time_obj:
            start_time = earliest_utc_obj
        
    """ Execute query """
    campaigns, all_data = crl.run_query({'metric_name':'earliest_timestamp','start_time':start_time,'end_time':end_time})
        
    sorted_campaigns = sorted(campaigns.iteritems(), key=operator.itemgetter(1))
    sorted_campaigns.reverse()
    
    if filter_data:
        try:
            
            if min_donations_var == '':
                min_donations = -1
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
            # timestamp = TP.timestamp_from_obj(all_data[key][3], 2, 2)
            timestamp = TP.timestamp_convert_format(all_data[key][3], 1, 2)
            
            if filter_data: 
                if all_data[key][2] > min_donations and int(all_data[key][3]) > earliest_utc_ts:
                    new_sorted_campaigns.append([campaign[0], campaign[1], name, timestamp, all_data[key][2], all_data[key][4]])
            else:
                new_sorted_campaigns.append([campaign[0], campaign[1], name, timestamp, all_data[key][2], all_data[key][4]])
    
    sorted_campaigns = new_sorted_campaigns

    return render_to_response('campaigns/index.html', {'campaigns' : sorted_campaigns, 'err_msg' : err_msg, 'min_donations' : min_donations_var, 'earliest_utc' : earliest_utc_ts_var}, context_instance=RequestContext(request))

    


"""

    Shows view stats over the time range for which the campaign receives landing page hits.  A form is also generated from the associated template
    that allows a test report to be generated.

"""
def show_campaigns(request, utm_campaign):
    
    """ Look 10 hrs into the past? """
    # end_time, start_time = TP.timestamps_for_interval(datetime.datetime.now() + datetime.timedelta(hours=8), 1, hours=-24)
    
    #start_time = '20101230130400'
    #end_time = '20101230154400' 
    """ currently the start time is hard coded to the beginning of FR testing """
    start_time = '20110531120000'  
    end_time = TP.timestamp_from_obj(datetime.datetime.now() + datetime.timedelta(hours=8),1,3)
    
    interval = 1
        
    """ Estimate start/end time of campaign """
    """ This generates an image for campaign views """
    ir = DR.IntervalReporting(was_run=False, use_labels=False, font_size=20, plot_type='line', query_type='campaign', file_path=projSet.__web_home__ + 'campaigns/static/images/')
    
    """ 
        Try to produce analysis on the campaign view data  
        If unsuccessful reverse to index view and emit error message
    """
    try:
        ir.run(start_time, end_time, interval, 'views', utm_campaign, {})
    
    except Exception as inst:
        print >> sys.stderr, type(inst)     # the exception instance
        print >> sys.stderr, inst.args      # arguments stored in .args
        print >> sys.stderr, inst           # __str__ allows args to printed directly
        
        """ !! FIXME / TODO - when reversing POST an error message also !! """
        # err_msg = 'There is insufficient data to analyze this campaign %s.' % utm_campaign
        # return HttpResponseRedirect(reverse('campaigns.views.index'))
    
    """ search for start_time and end_time """
    # top_view_interval = max(ir._counts_[utm_campaign])

    start_count = 0
    end_count = len(ir._counts_[utm_campaign]) 
    
    """ 
        ESTIMATE THE START AND END TIME OF THE CAMAPIGN
        
        Search for the first instance when more than 10 views are observed over a smapling period
    """
    
    num_datapoints = len(ir._counts_[utm_campaign])
    range_list = range(num_datapoints)
    
    for i in range_list:
        # if i > (0.5 * top_view_interval) and not(begin_count):
        if ir._counts_[utm_campaign][i] > 10:
            start_count = i
            start_count = ir._times_[utm_campaign][i]
            break    
    
    range_list.reverse()
    for i in range_list:
        
        if ir._counts_[utm_campaign][i] > 10:
            end_count = i
            end_count = ir._times_[utm_campaign][i]
            break    

    """ Based on where the first and last number of views/interval are observed to be greater that 10, generate the associated timestamps """
    
    start_time_est = TP.timestamp_to_obj(start_time, 1) + datetime.timedelta(minutes=start_count)
    end_time_est = TP.timestamp_to_obj(start_time, 1) + datetime.timedelta(minutes=end_count)

    """ Really bad hack for the moment - subtract a day from times """
    """ use for campaigns: C_JMvJD_Junetest_EN, C_JMvJD_Junetest_US """
    start_time_est = start_time_est + datetime.timedelta(days=-1)
    end_time_est = end_time_est + datetime.timedelta(days=-1)
    
    start_time_est = TP.timestamp_from_obj(start_time_est, 1, 2)
    end_time_est = TP.timestamp_from_obj(end_time_est, 1, 2)
    

    
    """ Read the test name """
    ttl = DL.TestTableLoader()
    row = ttl.get_test_row(utm_campaign)
    test_name = ttl.get_test_field(row ,'test_name')
    
    """ Regenerate the data using the estimated start and end times """
    ir = DR.IntervalReporting(was_run=False, use_labels=False, font_size=20, plot_type='line', query_type='campaign', file_path=projSet.__web_home__ + 'campaigns/static/images/')
    ir.run(start_time_est, end_time_est, interval, 'views', utm_campaign, {})

        
    """ determine the type of test """
    """ Get the banners  """
    test_type, artifact_name_list = FDH.get_test_type(utm_campaign, start_time, end_time, DL.CampaignReportingLoader(''))
    
    return render_to_response('campaigns/show_campaigns.html', {'utm_campaign' : utm_campaign, 'test_name' : test_name, 'start_time' : start_time_est, 'end_time' : end_time_est, 'artifacts' : artifact_name_list, 'test_type' : test_type}, context_instance=RequestContext(request))    
    
    # return HttpResponseRedirect(reverse('campaigns.views.index'))

    """ return a form that displays estimates and allows a test to be generated """


"""
    
    Retrieve a report from the database and display

"""
def show_report(request, utm_campaign):
    
    ttl = DL.TestTableLoader()
    
    row = ttl.get_test_row(utm_campaign)
    
    html = row[7]

    return HttpResponse(html)

    
    