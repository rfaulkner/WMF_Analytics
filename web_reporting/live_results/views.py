
"""
    DJANGO VIEW DEFINITIONS:
    ========================
    
    Defines the views for ongoing donations.  This view interacts with templates serving embedded AJAX plotting to provide the user with a 
    a running feed of campaign, banner, and landing page performance based on donations received.

    Views:
    
    index             -- Queries the fundraiser database for donations and sends the data to the template to be displayed 
    
    impression_list     - listing of impressions for various countries
    
    
    Helpers:
    
    get_data_lists        -- composes the donation query data into a format expected by the template
    combine_data_lists    -- combines the separate data sets from different queries into one in a format expected by the template 
    
"""

__author__ = "Ryan Faulkner"
__revision__ = "$Revision$"
__date__ = "June 20th, 2011"

""" Import django modules """
from django.shortcuts import render_to_response
from django.template import RequestContext


""" Import python base modules """
import datetime, logging, sys, MySQLdb, re

""" Import Analytics modules """
import classes.Helper as Hlp
import classes.DataReporting as DR
import classes.DataLoader as DL
import classes.DataCaching as DC
import classes.TimestampProcessor as TP
import classes.QueryData as QD
import classes.FundraiserDataHandler as FDH
import config.settings as projSet
import data.web_reporting_view_keys as view_keys

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

"""
    Index page for live results. 
"""
def index(request):
    
    
    """ 
        PROCESS POST DATA
        ================= 
        
        Escape all user input that can be entered in text fields 
        
    """
    try:
        campaign_regexp_filter = MySQLdb._mysql.escape_string(request.POST['campaign_regexp_filter'])
        
        if cmp(campaign_regexp_filter, '') == 0:
            campaign_regexp_filter = '^C_|^C11_'
    except:
        campaign_regexp_filter = '^C_|^C11_'

    try:
        min_donation = MySQLdb._mysql.escape_string(request.POST['min_donation'].strip())
        min_donation = int(min_donation)
    
    except:
        min_donation = 0
    
    # Filter on ISO codes to include matched countries
    try:
        iso_filter = MySQLdb._mysql.escape_string(request.POST['iso_filter'].strip())        
    
    except:
        iso_filter = '.{2}'
        
    
    """
        Call up cached results
    """     
    
    cache = DC.LiveResults_DataCaching()
    dict_param = cache.retrieve_cached_data(view_keys.LIVE_RESULTS_DICT_KEY)
    
    measured_metrics_counts = dict_param['measured_metrics_counts']
    results = dict_param['results']
    column_names = dict_param['column_names']
    sampling_interval = dict_param['interval']    
    duration_hrs = dict_param['duration']
    
    start_time = dict_param['start_time']
    end_time = dict_param['end_time']
        
    ir_cmpgn = DR.IntervalReporting(query_type=FDH._QTYPE_CAMPAIGN_ + FDH._QTYPE_TIME_, generate_plot=False)
    ir_banner = DR.IntervalReporting(query_type=FDH._QTYPE_BANNER_ + FDH._QTYPE_TIME_, generate_plot=False)
    ir_lp = DR.IntervalReporting(query_type=FDH._QTYPE_LP_ + FDH._QTYPE_TIME_, generate_plot=False)
    
    ir_cmpgn._counts_ = dict_param['ir_cmpgn_counts']
    ir_banner._counts_ = dict_param['ir_banner_counts']
    ir_lp._counts_ = dict_param['ir_lp_counts']
    
    ir_cmpgn._times_ = dict_param['ir_cmpgn_times']
    ir_banner._times_ = dict_param['ir_banner_times']
    ir_lp._times_ = dict_param['ir_lp_times']

    metric_legend_table = dict_param['metric_legend_table']
    conf_legend_table = dict_param['conf_legend_table']

    
    """ Filtering -- donations and artifacts """
    
    country_index = column_names.index('country')
    donations_index = column_names.index('donations')
    campaign_index = column_names.index('utm_campaign')
    new_results = list()
    
    # minimum d
    for row in results:
        try:
            if row[donations_index] > min_donation and re.search(campaign_regexp_filter, row[campaign_index]) and re.search(iso_filter, row[country_index]):
                new_results.append(list(row))
        except:
            logging.error('live_results/views.py -- Could not process row: %s' % str(row))

    results = new_results
    
    new_measured_metrics_counts = dict()
    for metric in measured_metrics_counts:        
        new_measured_metrics_counts[metric] = dict()
        
        for artifact_key in measured_metrics_counts[metric]:
            if re.search(campaign_regexp_filter, artifact_key):
                new_measured_metrics_counts[metric][artifact_key] = measured_metrics_counts[metric][artifact_key]
         
    """ 
        Format results to encode html table cell markup in results        
    """

    ret = DR.ConfidenceReporting(query_type='', hyp_test='').get_confidence_on_time_range(None, None, None, measured_metrics_counts=new_measured_metrics_counts) # first get color codes on confidence
    conf_colour_code = ret[0]
    
    for row_index in range(len(results)):
        artifact_index = results[row_index][0] + '-' + results[row_index][1] + '-' + results[row_index][2]
        
        for col_index in range(len(column_names)):
            
            is_coloured_cell = False
            if column_names[col_index] in conf_colour_code.keys():
                if artifact_index in conf_colour_code[column_names[col_index]].keys():
                    results[row_index][col_index] = '<td style="background-color:' + conf_colour_code[column_names[col_index]][artifact_index] + ';">' + str(results[row_index][col_index]) + '</td>'
                    is_coloured_cell = True
                    
            if not(is_coloured_cell):
                results[row_index][col_index] = '<td>' + str(results[row_index][col_index]) + '</td>'
                
    if results:
        summary_table = DR.DataReporting()._write_html_table(results, column_names, use_standard_metric_names=True, omit_cell_markup=True)
    else:
        summary_table = '<p><font size="4">No data available.</font></p>'
        
    summary_table = '<h4><u>Metrics Legend:</u></h4><div class="spacer"></div>' + metric_legend_table + \
    '<div class="spacer"></div><h4><u>Confidence Legend for Hypothesis Testing:</u></h4><div class="spacer"></div>' + conf_legend_table + '<div class="spacer"></div><div class="spacer"></div>' + summary_table


        
    """ 
        Prepare Live Plots
    """
    
    """ compose a list of zero data """    
    empty_data = [[1.0, 0.0]] * (duration_hrs * 60 / sampling_interval + 1)
    for i in range(len(empty_data)):
        empty_data[i][0] = empty_data[i][0] * i *  sampling_interval
        
    """ Extract data from interval reporting objects """        
    cmpgn_data_dict = ir_cmpgn.get_data_lists(['C_', 'C11_', campaign_regexp_filter], empty_data)
    cmpgn_banner_dict = ir_banner.get_data_lists(['B_', 'B11_'], empty_data)
    cmpgn_lp_dict = ir_lp.get_data_lists(['L11_', '^cc'], empty_data)
        
        
    """  
        Build template parameters
    """
    
    template_dict = Hlp.combine_data_lists([cmpgn_data_dict, cmpgn_banner_dict, cmpgn_lp_dict]) # combine the separate data sets
    template_dict['summary_table'] = summary_table
    template_dict['latest_log_end_time'] = end_time
    template_dict['start_time'] = start_time
    
    return render_to_response('live_results/index.html', template_dict, context_instance=RequestContext(request))


"""
    View for Long Term results over all Fundraising campaigns hour by hour
"""
def long_term_trends(request):
    
    cache = DC.LTT_DataCaching()
    dict_param = cache.retrieve_cached_data(view_keys.LTT_DICT_KEY)
    
    return render_to_response('live_results/long_term_trends.html', dict_param, context_instance=RequestContext(request))


"""
    View for Long Term results over all Fundraising campaigns hour by hour
"""
def fundraiser_totals(request):
    
    cache = DC.Fundraiser_Totals_DataCaching()
    data = cache.retrieve_cached_data(view_keys.FR_TOT_DICT_KEY)
    
    dict_param = data['Total']
    dict_param['err_msg'] = ''
    dict_param['country'] = 'Total'
    
    return render_to_response('live_results/fundraiser_totals.html', dict_param, context_instance=RequestContext(request))

"""
    View for Long Term results over all Fundraising campaigns hour by hour
"""
def fundraiser_totals_cntry(request, country):
    
    cache = DC.Fundraiser_Totals_DataCaching()
    data = cache.retrieve_cached_data(view_keys.FR_TOT_DICT_KEY)
    
    try: 
        dict_param_country = data[country]
        dict_param_country['err_msg'] = ''
        dict_param_country['country'] = country
        return render_to_response('live_results/fundraiser_totals.html', dict_param_country, context_instance=RequestContext(request))
    
    except:
        dict_param = data['Total']
        dict_param['err_msg'] = 'Could not find totals for that country.'
        dict_param['country'] = 'Total'
        return render_to_response('live_results/fundraiser_totals.html', dict_param, context_instance=RequestContext(request))

"""
    Generates a listing of impressions for all countries and banners
"""
def impression_list(request):
    
    err_msg = ''
    where_clause = ''
    
    """ 
        Process times and POST
        =============
    """
    duration_hrs = 2
    end_time, start_time = TP.timestamps_for_interval(datetime.datetime.utcnow(), 1, hours=-duration_hrs)    
    
    if 'earliest_utc_ts' in request.POST:
        if cmp(request.POST['earliest_utc_ts'], '') != 0:
            earliest_utc_ts = MySQLdb._mysql.escape_string(request.POST['earliest_utc_ts'].strip())
            format = TP.getTimestampFormat(earliest_utc_ts)
            
            if format == 1:
                start_time = earliest_utc_ts
            if format == 2:
                start_time = TP.timestamp_convert_format(earliest_utc_ts, 2, 1)
            elif format == -1:
                err_msg = err_msg + 'Start timestamp is formatted incorrectly\n'
    
    if 'latest_utc_ts' in request.POST:
        if cmp(request.POST['latest_utc_ts'], '') != 0:
            latest_utc_ts = MySQLdb._mysql.escape_string(request.POST['latest_utc_ts'].strip())
            format = TP.getTimestampFormat(latest_utc_ts)
            
            if format == 1:
                end_time = latest_utc_ts
            if format == 2:
                end_time = TP.timestamp_convert_format(latest_utc_ts, 2, 1)
            elif format == -1:
                err_msg = err_msg + 'End timestamp is formatted incorrectly\n'
            
    if 'iso_code' in request.POST:
        if cmp(request.POST['iso_code'], '') != 0:
            iso_code = MySQLdb._mysql.escape_string(request.POST['iso_code'].strip())
            where_clause = "where bi.country regexp '%s' " % iso_code
                    
    """ 
        Format and execute query 
        ========================
    """
        
    query_name = 'report_country_impressions.sql'    
    
    sql_stmnt = Hlp.file_to_string(projSet.__sql_home__ + query_name)
    sql_stmnt = sql_stmnt % (start_time, end_time, start_time, end_time, start_time, end_time, where_clause)
    
    dl = DL.DataLoader()
    results = dl.execute_SQL(sql_stmnt)
    column_names = dl.get_column_names()

    imp_table = DR.DataReporting()._write_html_table(results, column_names)
    
    return render_to_response('live_results/impression_list.html', {'imp_table' : imp_table.decode("utf-8"), 'err_msg' : err_msg, 'start' : TP.timestamp_convert_format(start_time, 1, 2), 'end' : TP.timestamp_convert_format(end_time, 1, 2)},  context_instance=RequestContext(request))


"""
    Generates results for day to day totals by country
"""
def daily_totals(request):    

    err_msg = ''
    
    start_day_ts = TP.timestamp_from_obj(datetime.datetime.utcnow() + datetime.timedelta(days=-1), 1, 0)
    end_day_ts = TP.timestamp_from_obj(datetime.datetime.utcnow(), 1, 0)
    country = '.{2}'
    min_donation = 0
    order_str = 'order by 1 desc,3 desc'
    
    """
        PROCESS POST
    """
    
    if 'start_day_ts' in request.POST:
        if cmp(request.POST['start_day_ts'], '') != 0:
            start_day_ts = MySQLdb._mysql.escape_string(request.POST['start_day_ts'].strip())
            format = TP.getTimestampFormat(start_day_ts)
            
            if format == 2:
                start_day_ts = TP.timestamp_convert_format(start_day_ts, 2, 1)
                # start_day_ts = start_day_ts[:8] + '000000'
            elif format == -1:
                err_msg = err_msg + 'Start timestamp is formatted incorrectly\n'

    if 'end_day_ts' in request.POST:
        if cmp(request.POST['end_day_ts'], '') != 0:
            end_day_ts = MySQLdb._mysql.escape_string(request.POST['end_day_ts'].strip())
            format = TP.getTimestampFormat(start_day_ts)
            
            if format == 2:
                end_day_ts = TP.timestamp_convert_format(end_day_ts, 2, 1)
                # end_day_ts = end_day_ts[:8] + '000000'
            elif format == -1:
                err_msg = err_msg + 'End timestamp is formatted incorrectly\n'
            
    if 'country' in request.POST:
        if cmp(request.POST['country'], '') != 0:
            country = MySQLdb._mysql.escape_string(request.POST['country'])

    if 'min_donation' in request.POST:
        if cmp(request.POST['min_donation'], '') != 0:
            try:                
                min_donation = int(MySQLdb._mysql.escape_string(request.POST['min_donation'].strip()))
            except:
                logging.error('live_results/daily_totals -- Could not process minimum donation for "%s" ' % request.POST['min_donation'].strip())
                min_donation = 0
    
    if 'order_metric' in request.POST:
        if cmp(request.POST['order_metric'], 'Date') == 0:
            order_str = 'order by 1 desc,3 desc'
        elif cmp(request.POST['order_metric'], 'Country') == 0:
            order_str = 'order by 2 asc,1 desc'
            
    """
        === END POST ===
    """
    
    query_name = 'report_daily_totals_by_country'
    filename = projSet.__sql_home__+ query_name + '.sql'
    sql_stmnt = Hlp.file_to_string(filename)
    sql_stmnt = QD.format_query(query_name, sql_stmnt, [start_day_ts, end_day_ts], country=country, min_donation=min_donation, order_str=order_str)
    
    dl = DL.DataLoader()    
    results = dl.execute_SQL(sql_stmnt)
    html_table = DR.DataReporting()._write_html_table(results, dl.get_column_names(), use_standard_metric_names=True)
    
    return render_to_response('live_results/daily_totals.html', \
                              {'html_table' : html_table, 'start_time' : TP.timestamp_convert_format(start_day_ts, 1, 2), 'end_time' : TP.timestamp_convert_format(end_day_ts, 1, 2)}, \
                              context_instance=RequestContext(request))

