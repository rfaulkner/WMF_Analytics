"""
    DJANGO VIEW DEFINITIONS:
    ========================
    
    Defines the views for Log Miner Logging (LML) application.  The LML is meant to provide functionality for observing log mining activity and to
    copy and mine logs at the users request.

    Views:
    
    index             -- the index page shows a listing of loaded/mined logs by start time and and the time of log mining  
    copy_logs_form    -- this renders a form where the user can request logs to be copied over for a certain hour
    copy_logs_process -- the target of the copy form that executes the copying using a DataMapper
    log_list          -- Shows a listing of all copied logs.  The links allow mining to be initiated.  
    mine_logs_form    -- this renders a form where the user can request logs of a certain hour timestamop to be mined
    mine_logs_process -- the target of the mine form that executes the mining using a DataMapper
    mine_logs_process_file    -- target of the link from the log_list page ... mines log associated to link via DataMapper
"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "June 20th, 2011"

""" Import django modules """
from django.shortcuts import render_to_response
from django.template import RequestContext

""" Import python base modules """
import MySQLdb, logging, sys, datetime

""" Import Analytics modules """
import classes.DataLoader as DL
import classes.DataReporting as DR
import classes.TimestampProcessor as TP



""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


"""
    Index page for finding the latest camapigns.  Displays a list of recent campaigns with more than k donations over the last n hours. 
    
"""
def index(request):
    
    err_msg, earliest_utc_ts_var, latest_utc_ts_var = process_filter_data(request)
    
    sltl = DL.SquidLogTableLoader()
    
    """ Show the squid log table """
    squid_table = sltl.get_all_rows_unique_start_time()
    filtered_squid_table = list()
    
    for row in squid_table:
         
        log_start_time = sltl.get_squid_log_record_field(row, 'start_time')
        
        """ Ensure the timestamp is properly formatted """
        if TP.is_timestamp(log_start_time, 2):
            log_start_time = TP.timestamp_convert_format(log_start_time, 2, 1)
            
        if int(log_start_time) > int(earliest_utc_ts_var) and int(log_start_time) < int(latest_utc_ts_var):
            filtered_squid_table.append(row)
    
    squid_table = filtered_squid_table
    squid_table.reverse()
    column_names = sltl.get_column_names()
    new_column_names = list()
    
    for name in column_names:
        new_column_names.append(sltl.get_verbose_column(name))
        
    squid_table = DR.DataReporting()._write_html_table(squid_table, new_column_names)
    
    """ Show the latest log that has been or is loading and its progress """
    completion_rate = sltl.get_completion_rate_of_latest_log()
    
    return render_to_response('LML/index.html', {'err_msg' : err_msg, 'squid_table' : squid_table, 'completion_rate' : completion_rate},  context_instance=RequestContext(request))


"""
    Generic timestamp filter processing
"""
def process_filter_data(request):
        
    err_msg = ''
    
    time_curr = datetime.datetime.utcnow()
    time_dayback = time_curr + datetime.timedelta(hours = -4)
     
    _beginning_time_ = TP.timestamp_from_obj(time_dayback, 1, 3)
    _end_time_ = TP.timestamp_from_obj(time_curr, 1, 3)
    
    
    """ 
        PROCESS POST VARS 
        =================
    """
    
    try:
        
        latest_utc_ts_var = MySQLdb._mysql.escape_string(request.POST['latest_utc_ts'].strip())
        
        if not(TP.is_timestamp(latest_utc_ts_var, 1)) and not(TP.is_timestamp(latest_utc_ts_var, 2)):
            raise TypeError

        if latest_utc_ts_var == '':
            latest_utc_ts_var = _end_time_
        
        ts_format = TP.getTimestampFormat(latest_utc_ts_var)
        if ts_format == TP.TS_FORMAT_FORMAT1:
            latest_utc_ts_var = TP.timestamp_convert_format(latest_utc_ts_var, TP.TS_FORMAT_FORMAT1, TP.TS_FORMAT_FLAT)
            
    except KeyError:        
        latest_utc_ts_var = _end_time_
    
    except TypeError:        
        err_msg = 'Please enter a valid end-timestamp.'        
        latest_utc_ts_var = _end_time_


    try:
        
        earliest_utc_ts_var = MySQLdb._mysql.escape_string(request.POST['earliest_utc_ts'].strip())
        
        if not(TP.is_timestamp(earliest_utc_ts_var, 1)) and not(TP.is_timestamp(earliest_utc_ts_var, 2)):
            raise TypeError

        if earliest_utc_ts_var == '':
            earliest_utc_ts_var = _beginning_time_
        
        ts_format = TP.getTimestampFormat(earliest_utc_ts_var)
        if ts_format == TP.TS_FORMAT_FORMAT1:
            earliest_utc_ts_var = TP.timestamp_convert_format(earliest_utc_ts_var, TP.TS_FORMAT_FORMAT1, TP.TS_FORMAT_FLAT)
            
    except KeyError:
        earliest_utc_ts_var = _beginning_time_
    
    except TypeError:        
        err_msg = 'Please enter a valid start-timestamp.'        
        earliest_utc_ts_var = _beginning_time_
        
        
    return err_msg, earliest_utc_ts_var, latest_utc_ts_var


"""
    Display and enable user to 
"""
def mining_patterns_view(request):
    
    mptl = DL.MiningPatternsTableLoader()
    
    banner_patterns, lp_patterns = mptl.get_pattern_lists()
            
    return render_to_response('LML/mining_patterns.html', {'banner_patterns' : banner_patterns, 'lp_patterns' : lp_patterns},  context_instance=RequestContext(request))


"""
    Display and enable user to add new patterns
"""
def mining_patterns_add(request):
    
    err_msg = ''
    mptl = DL.MiningPatternsTableLoader()
    
    type = 'banner'
    
    """ Extract Post data """
    try:
        regexp = MySQLdb._mysql.escape_string(request.POST['regexp_pattern'])
        type = MySQLdb._mysql.escape_string(request.POST['pattern_type'])
        
    except:
        
        err_msg = 'Fields to add mining pattern incorrect.'
        pass 
        
    mptl.insert_row(pattern_type=type, pattern=regexp)
    banner_patterns, lp_patterns = mptl.get_pattern_lists()
    
    return render_to_response('LML/mining_patterns.html', {'err_msg' : err_msg , 'banner_patterns' : banner_patterns, 'lp_patterns' : lp_patterns},  context_instance=RequestContext(request))

        
"""
    Display and enable user to delete new patterns
"""
def mining_patterns_delete(request):
    
    mptl = DL.MiningPatternsTableLoader()
    
    try:    
        banner_patterns = request.POST.getlist('banner_patterns')
        
        """ Escape POST input """
        for index in range(len(banner_patterns)):
            banner_patterns[index] = MySQLdb._mysql.escape_string(str(banner_patterns[index]))
    
    except:
        banner_patterns = list()
        pass
    
    try:
        lp_patterns =  request.POST.getlist('lp_patterns')
        
        """ Escape POST input """
        for index in range(len(lp_patterns)):
            lp_patterns[index] = MySQLdb._mysql.escape_string(str(lp_patterns[index]))
            
    except:        
        lp_patterns = list()
        pass
                
    logging.debug(banner_patterns)
    logging.debug(lp_patterns)
    
    """ Remove selected patterns """
    for elem in banner_patterns:
        mptl.delete_row(pattern=elem,pattern_type='banner')
    
    for elem in lp_patterns:
        mptl.delete_row(pattern=elem,pattern_type='lp')
        
    banner_patterns, lp_patterns = mptl.get_pattern_lists()
    
    return render_to_response('LML/mining_patterns.html', {'banner_patterns' : banner_patterns, 'lp_patterns' : lp_patterns},  context_instance=RequestContext(request))



