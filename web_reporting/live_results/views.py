
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
import datetime, logging, sys, MySQLdb

""" Import Analytics modules """
import classes.Helper as Hlp
import classes.DataReporting as DR
import classes.DataLoader as DL
import classes.FundraiserDataHandler as FDH
import classes.TimestampProcessor as TP
import config.settings as projSet

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

"""
    Index page for live results. 
"""
def index(request):
    
    """ Find the earliest and latest page views for a given campaign  """
    lptl = DL.LandingPageTableLoader()
    
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
                
    if 'country_filter' in request.POST:        
        query_name = 'report_summary_results_country.sql'
        query_name_1S = 'report_summary_results_country_1S.sql'
        
    else:
        query_name = 'report_summary_results.sql'
        query_name_1S = 'report_summary_results_1S.sql'
        
        
    """ Get the donations for all campaigns over the last n hours """
    duration_hrs = 6
    sampling_interval = 5
    dl = DL.DataLoader()
    end_time, start_time = TP.timestamps_for_interval(datetime.datetime.utcnow(), 1, hours=-duration_hrs)
    # start_time = '20111102220000'
    # end_time = '20111103040000'
    
    """ Should a one-step query be used? """        
    use_one_step = lptl.is_one_step(start_time, end_time, 'C11')  # Assume it is a one step test if there are no impressions for this campaign in the landing page table
    
    """ 
        Retrieve the latest time for which impressions have been loaded
    """
    
    sql_stmnt = 'select max(end_time) as latest_ts from squid_log_record where log_completion_pct = 100.00'
    
    results = dl.execute_SQL(sql_stmnt)
    latest_timestamp = results[0][0]
    latest_timestamp = TP.timestamp_from_obj(latest_timestamp, 2, 3)
    latest_timestamp_flat = TP.timestamp_convert_format(latest_timestamp, 2, 1)

    try: 
        if campaign_regexp_filter != '^C_|^C11_':
            conf_colour_code = DR.ConfidenceReporting(query_type='', hyp_test='').get_confidence_on_time_range(start_time, end_time, campaign_regexp_filter, one_step=use_one_step)
        else:
            conf_colour_code = {}
    except:
        conf_colour_code = {}
    
    """ 
        Prepare Live Tables 
        ===================
    """
    
    sql_stmnt = Hlp.file_to_string(projSet.__sql_home__ + query_name)
    sql_stmnt = sql_stmnt % (start_time, latest_timestamp_flat, start_time, latest_timestamp_flat, campaign_regexp_filter, start_time, latest_timestamp_flat, campaign_regexp_filter, \
                             start_time, end_time, campaign_regexp_filter, start_time, end_time, campaign_regexp_filter, start_time, end_time, campaign_regexp_filter, \
                             start_time, latest_timestamp_flat, campaign_regexp_filter, start_time, latest_timestamp_flat, campaign_regexp_filter)        
    
    logging.info('Executing report_summary_results ...')
    
    results = dl.execute_SQL(sql_stmnt)
    column_names = dl.get_column_names()
    
    if use_one_step:
        
        logging.info('... including one step artifacts ...')
        
        sql_stmnt_1S = Hlp.file_to_string(projSet.__sql_home__ + query_name_1S)
        sql_stmnt_1S = sql_stmnt_1S % (start_time, latest_timestamp_flat, start_time, latest_timestamp_flat, campaign_regexp_filter, start_time, latest_timestamp_flat, campaign_regexp_filter, \
                                 start_time, end_time, campaign_regexp_filter, start_time, end_time, campaign_regexp_filter, start_time, end_time, campaign_regexp_filter, \
                                 start_time, latest_timestamp_flat, campaign_regexp_filter, start_time, latest_timestamp_flat, campaign_regexp_filter)
        
        results = list(results)        
        results_1S = dl.execute_SQL(sql_stmnt_1S)
        
        """ Ensure that the results are unique """
        one_step_keys = list()
        for row in results_1S:
            one_step_keys.append(str(row[0]) + str(row[1]) + str(row[2]))
        
        new_results = list()
        for row in results:
            key = str(row[0]) + str(row[1]) + str(row[2])
            if not(key in one_step_keys):
                new_results.append(row)
        results = new_results
            
        results.extend(list(results_1S))
    
    """ Filtering -- remove rows with fewer than 5 donations """
    donations_index = column_names.index('donations')
    new_results = list()
    # min_donation = 1
    
    for row in results:
        if row[donations_index] > min_donation:
            new_results.append(list(row))
    
    results = new_results
            
    """ 
        Format results to encode html table cell markup in results
        ===============================
        
    """
    for row_index in range(len(results)):
        artifact_index = results[row_index][1] + '-' + results[row_index][2]
        
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
        
    metric_legend_table = DR.DataReporting().get_standard_metrics_legend()
    conf_legend_table = DR.ConfidenceReporting(query_type='bannerlp', hyp_test='TTest').get_confidence_legend_table()
    summary_table = '<h4><u>Metrics Legend:</u></h4><div class="spacer"></div>' + metric_legend_table + \
    '<div class="spacer"></div><h4><u>Confidence Legend for Hypothesis Testing:</u></h4><div class="spacer"></div>' + conf_legend_table + '<div class="spacer"></div><div class="spacer"></div>' + summary_table

        
    """ 
        Prepare Live Plots 
        ==================
    """
    
    """ compose a list of zero data """    
    empty_data = [[1.0, 0.0]] * (duration_hrs * 60 / sampling_interval + 1)
    for i in range(len(empty_data)):
        empty_data[i][0] = empty_data[i][0] * i *  sampling_interval
        
    """ Create a interval loader objects """
    ir_cmpgn = DR.IntervalReporting(query_type=FDH._QTYPE_CAMPAIGN_ + FDH._QTYPE_TIME_, generate_plot=False)
    ir_banner = DR.IntervalReporting(query_type=FDH._QTYPE_BANNER_ + FDH._QTYPE_TIME_, generate_plot=False)
    ir_lp = DR.IntervalReporting(query_type=FDH._QTYPE_LP_ + FDH._QTYPE_TIME_, generate_plot=False)
        
    """ Execute queries for campaign, banner, and landing page donations """        
    #ir.run('20110603120000', '20110604000000', 2, 'donations', '',[])
    ir_cmpgn.run(start_time, end_time, sampling_interval, 'donations', '',{})
    ir_banner.run(start_time, end_time, sampling_interval, 'donations', '',{})
    ir_lp.run(start_time, end_time, sampling_interval, 'donations', '',{})
    
    """ Extract data from interval reporting objects """        
    cmpgn_data_dict = ir_cmpgn.get_data_lists(['C_', 'C11_'], empty_data)
    cmpgn_banner_dict = ir_banner.get_data_lists(['B_', 'B11_'], empty_data)
    cmpgn_lp_dict = ir_lp.get_data_lists(['L11_', '^cc'], empty_data)
        
    """ combine the separate data sets """
    dict_param = Hlp.combine_data_lists([cmpgn_data_dict, cmpgn_banner_dict, cmpgn_lp_dict])
    dict_param['summary_table'] = summary_table
    dict_param['latest_log_end_time'] = latest_timestamp
    dict_param['start_time'] = TP.timestamp_convert_format(start_time, 1, 2)
    
    return render_to_response('live_results/index.html', dict_param,  context_instance=RequestContext(request))


"""
    View for Long Term results over all Fundraising campaigns hour by hour
"""
def long_term_trends(request):
    
    """ number of hours to look back """
    duration_hrs = 72
    
    end_time, start_time = TP.timestamps_for_interval(datetime.datetime.utcnow() + datetime.timedelta(minutes=-20), 1, hours=-duration_hrs)
    
    """ set the metrics to plot """
    lttdl = DL.LongTermTrendsLoader()
    metrics = ['impressions', 'views', 'donations', 'amount', 'click_rate']
    metrics_index = [0, 1, 2, 2, 3]
    
    country_groups = {'US':'(US)', 'CA':'(CA)', 'JP':'(JP)', 'IN':'(IN)', 'NL':'(NL)', 'Other':'(US|CA|JP|IN|NL)'}
    currency_groups = {'USD':'(USD)', 'CAD':'(CAD)', 'JPY':'(JPY)', 'EUR':'(EUR)', 'Other':'(USD|CAD|JPY|EUR)'}
    groups = [country_groups, country_groups, country_groups, country_groups, country_groups]
    group_metrics = ['country', 'country', 'country', 'country', 'country']
    
    metric_types = [DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_RATE_]
    data = list()
    
    """ For each metric use the LongTermTrendsLoader to generate the data to plot """
    for index in range(len(metrics)):
        
        dr = DR.DataReporting()
        
        times, counts = lttdl.run_query(start_time, end_time, metrics_index[index], metric_name=metrics[index], metric_type=metric_types[index], groups=groups[index], group_metric=group_metrics[index])
        times = TP.normalize_timestamps(times, False, 1)
            
        dr._counts_ = counts
        dr._times_ = times

        empty_data = [0] * len(times['Total'])
        data.append(dr.get_data_lists([''], empty_data))
    
    dict_param = Hlp.combine_data_lists(data)
    
    return render_to_response('live_results/long_term_trends.html', dict_param,  context_instance=RequestContext(request))



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
            where_clause = "where bi.country = '%s' " % iso_code
                    
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
