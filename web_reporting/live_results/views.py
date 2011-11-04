
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
    
    """ Get the donations for all campaigns over the last n hours """
    duration_hrs = 4
    sampling_interval = 10
    dl = DL.DataLoader()
    end_time, start_time = TP.timestamps_for_interval(datetime.datetime.utcnow(), 1, hours=-duration_hrs)
    # start_time = '20111028170000'
    # end_time = '20111028210000'
    
    """ 
        Retrieve the latest time for which impressions have been loaded
    """
    
    sql_stmnt = 'select max(end_time) as latest_ts from squid_log_record where log_completion_pct = 100.00'
    
    results = dl.execute_SQL(sql_stmnt)
    latest_timestamp = results[0][0]
    latest_timestamp = TP.timestamp_from_obj(latest_timestamp, 2, 3)
    latest_timestamp_flat = TP.timestamp_convert_format(latest_timestamp, 2, 1)

    try: 
        conf_colour_code = DR.ConfidenceReporting(query_type='', hyp_test='').get_confidence_on_time_range(start_time, end_time, campaign_regexp_filter)
        
    except:
        conf_colour_code = {}
    
    """ 
        Prepare Live Tables 
        ===================
    """
    
    sql_stmnt = Hlp.file_to_string(projSet.__sql_home__ + 'report_summary_results.sql')
    sql_stmnt = sql_stmnt % (start_time, latest_timestamp_flat, start_time, latest_timestamp_flat, campaign_regexp_filter, start_time, latest_timestamp_flat, campaign_regexp_filter, \
                             start_time, end_time, campaign_regexp_filter, start_time, end_time, campaign_regexp_filter, start_time, end_time, campaign_regexp_filter, \
                             start_time, latest_timestamp_flat, campaign_regexp_filter, start_time, latest_timestamp_flat, campaign_regexp_filter)    
    
    logging.info('Executing report_summary_results ...')
    results = dl.execute_SQL(sql_stmnt)
    column_names = dl.get_column_names()
    
    """ Filtering -- remove rows with fewer than 5 donations """
    donations_index = column_names.index('donations')
    new_results = list()
    min_donation = 10
    
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
    

