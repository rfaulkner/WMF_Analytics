
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
import sys, datetime, re

""" Import Analytics modules """
import classes.Helper as Hlp
import classes.DataReporting as DR
import classes.DataLoader as DL
import classes.FundraiserDataHandler as FDH
import classes.TimestampProcessor as TP
import config.settings as projSet


"""
    Index page for live results. 
"""
def index(request):
    
    """ Get the donations for all campaigns over the last n hours """
    duration_hrs = 6
    sampling_interval = 10
    
    end_time, start_time = TP.timestamps_for_interval(datetime.datetime.now() + datetime.timedelta(hours=5), 1, hours=-duration_hrs)
    #start_time = '20110902150000'
    #end_time = '20110902210000'
    
    
    """ 
        Prepare Live Tables 
        ===================
    """
    
    sql_stmnt = Hlp.file_to_string(projSet.__sql_home__ + 'report_summary_results.sql')
    sql_stmnt = sql_stmnt % (start_time, end_time, start_time, end_time, start_time, end_time, start_time, end_time)
    dl = DL.DataLoader()
    
    results = dl.execute_SQL(sql_stmnt)
    column_names = dl.get_column_names()
    
    """ Filtering -- remove rows with fewer than 5 donations """
    donations_index = column_names.index('donations')
    new_results = list()
    min_donation = 5
    
    for row in results:
        if row[donations_index] > min_donation:
            new_results.append(row)
    results = new_results
    summary_table = DR.DataReporting()._write_html_table(results, column_names)
    
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
    
    return render_to_response('live_results/index.html', dict_param,  context_instance=RequestContext(request))
    

