
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
    
    sql_stmnt = Hlp.read_sql(projSet.__sql_home__ + 'report_live_results.sql')
    sql_stmnt = sql_stmnt % (start_time, end_time, start_time, end_time, start_time, end_time, start_time, end_time)
    dl = DL.DataLoader()
    
    results = dl.execute_SQL(sql_stmnt)
    column_names = dl.get_column_names()

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
    cmpgn_data_dict = get_data_lists(ir_cmpgn, ['C_', 'C11_'], empty_data)
    cmpgn_banner_dict = get_data_lists(ir_banner, ['B_', 'B11_'], empty_data)
    cmpgn_lp_dict = get_data_lists(ir_lp, ['L11_'], empty_data)
         
    """ combine the separate data sets """
    dict_param = combine_data_lists([cmpgn_data_dict, cmpgn_banner_dict, cmpgn_lp_dict])
    dict_param['summary_table'] = summary_table
    
    return render_to_response('live_results/index.html', dict_param,  context_instance=RequestContext(request))


"""

    Helper method that formats Reporting data for consumption by javascript in live_results/index.html
    
    @param ir: Interval Reporting object
    @param pattern: A set of regexp patterns on which data keys are matched to filter
    @param empty_data: A set of empty data to be used in case there is no usable data from the reporting object 
    
    @return: dictionary storing data for javascript processing
"""
def get_data_lists(ir, patterns, empty_data):
    
    """ Get metrics """
    data = list()
    labels = '!'
    counts = list()
    max_data = 0
    
    """ Find the key with the highest count """
    max = 0
    for key in ir._counts_.keys():
        val = sum(ir._counts_[key])
        if val > max:
            max = val
    
    """ Only add keys with enough counts """
    data_index = 0
    for key in ir._counts_.keys():
        
        isFormed = False
        for pattern in patterns:
            if key == None:
                isFormed = isFormed or re.search(pattern, '')
            else:
                isFormed = isFormed or re.search(pattern, key)
                
        if sum(ir._counts_[key]) > 0.01 * max and isFormed:
            
            data.append(list())
            
            if key == None or key == '':
                labels = labels + 'empty?'
            else:
                labels = labels + key + '?'
            
            counts.append(len(ir._counts_[key]))  
            
            for i in range(counts[data_index]):
                data[data_index].append([ir._times_[key][i], ir._counts_[key][i]])
                if ir._counts_[key][i] > max_data:
                    max_data = ir._counts_[key][i]
                    
            data_index = data_index + 1
        
    labels = labels + '!'
    
    """ Use the default empty data if there is none """
    if not data:
        return {'num_elems' : 1, 'counts' : [len(empty_data)], 'labels' : '!no_data?!', 'data' : empty_data, 'max_data' : 0.0}
    else:
        return {'num_elems' : data_index, 'counts' : counts, 'labels' : labels, 'data' : data, 'max_data' : max_data}
    

"""

    Helper method that combines dictionaries for consumption by javascript in live_results/index.html
    
    @param dict_list: A list of dictionaries with identical keys 
    
    @return: a dict where the elements of each key for of the input is combined into a list of elements
"""
def combine_data_lists(dict_list):
    
    try:
        template_keys = dict_list[0].keys()
        num_elems = len(dict_list)
    except:
        print >> sys.stderr, projSet.__web_home__ + '/live_results/views.py: No template data found.'
        return -1

    combined_dict = dict()
    
    for key in template_keys:
        key_list = list()
        for i in range(num_elems):
            key_list.append(dict_list[i][key])
        combined_dict[key] = key_list
        
    return combined_dict

