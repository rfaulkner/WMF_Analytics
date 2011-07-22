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
from django.http import HttpResponseRedirect, HttpResponse
from django.core.urlresolvers import reverse

""" Import python base modules """
import re, datetime, MySQLdb, logging, sys

""" Import Analytics modules """
import Fundraiser_Tools.classes.Helper as Hlp
import Fundraiser_Tools.classes.DataReporting as DR
import Fundraiser_Tools.classes.DataLoader as DL
import Fundraiser_Tools.classes.DataMapper as DM
import Fundraiser_Tools.classes.FundraiserDataThreading as FDT
import Fundraiser_Tools.classes.FundraiserDataHandler as FDH
import Fundraiser_Tools.classes.TimestampProcessor as TP
import Fundraiser_Tools.settings as projSet



""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

_beginning_time_ = '000001122334455'
_end_time_ = '99990011223344'


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
    
    """ Show the latest log that has been or is loading and its progress """
    completion_rate = sltl.get_completion_rate_of_latest_log()
    
    return render_to_response('LML/index.html', {'err_msg' : err_msg, 'squid_table' : squid_table, 'completion_rate' : completion_rate},  context_instance=RequestContext(request))


"""
    Display log on squid archive server with the option to copy
    
"""
def copy_logs_form(request):
    now = datetime.datetime.now() + datetime.timedelta(hours=7)
    return render_to_response('LML/copy_logs.html', {'year': now.year, 'month': now.month, 'day': now.day, 'hour': now.hour},  context_instance=RequestContext(request))

""" 
    !! MODIFY -- integrate this into a new thread !!
    
    1 Show a full file listing of the logs on the log server
    2 Enable selection of logs to copy over
"""
def copy_logs_process(request):

    try:
        
        year_var = MySQLdb._mysql.escape_string(request.POST['year'])
        month_var = MySQLdb._mysql.escape_string(request.POST['month'])
        day_var = MySQLdb._mysql.escape_string(request.POST['day'])
        hour_var = MySQLdb._mysql.escape_string(request.POST['hour'])
                
    except KeyError:
        """ flag an error here for the user """
        return HttpResponseRedirect(reverse('LML.views.index'))
        # pass
    
    """ Initialize the datamapper and then copy the banner and lp logs """
    dm = DM.DataMapper()
    dm.copy_logs('banner', year=year_var, month=month_var, day=day_var, hour=hour_var)
    dm.copy_logs('lp', year=year_var, month=month_var, day=day_var, hour=hour_var)
    
    
    return render_to_response('LML/log_list.html', {'log_file_list' : dm.get_list_of_logs()},  context_instance=RequestContext(request))


def log_list(request):
    
    err_msg, earliest_utc_ts_var, latest_utc_ts_var = process_filter_data(request)
    
    dm = DM.DataMapper()
    log_file_list = dm.get_list_of_logs()
    filtered_list = list()
    
    for log_file in log_file_list:
        
        log_start_time = dm.get_timestamps(log_file)[0]
        
        if int(log_start_time) > int(earliest_utc_ts_var) and int(log_start_time) < int(latest_utc_ts_var):
            filtered_list.append(log_file)
        
        log_file_list = filtered_list
    
    log_file_list.reverse()
    
    return render_to_response('LML/log_list.html', {'err_msg' : err_msg, 'log_file_list' : log_file_list},  context_instance=RequestContext(request))


""" 
    Process mining logs for a given hour - linked from the mine logs form
"""
def mine_logs_process(request):

    try:
        
        year_var = MySQLdb._mysql.escape_string(request.POST['year'])
        month_var = MySQLdb._mysql.escape_string(request.POST['month'])
        day_var = MySQLdb._mysql.escape_string(request.POST['day'])
        hour_var = MySQLdb._mysql.escape_string(request.POST['hour'])
                
    except KeyError:
        """ flag an error here for the user """
        return HttpResponseRedirect(reverse('LML.views.index'))
        # pass
    
    """ Initialize the datamapper and then copy the banner and lp logs """
    fdm = DM.FundraiserDataMapper()
    log_files_list = fdm.get_list_of_logs()
            
    date_string = year_var + '-' + month_var + '-' + day_var + '-' + hour_var
    
    for lf in log_files_list:
        if re.search(date_string, lf):
            FDT.MinerThread(lf).run()
            
    return HttpResponseRedirect(reverse('LML.views.index'))

    
"""
    Executed when the user selects a log file to mine
    
    Kicks off a thread to load a squid log into the db

"""
def mine_logs_process_file(request, log_name):

    FDT.MinerThread(log_name).run()
            
    return HttpResponseRedirect(reverse('LML.views.index'))


"""
    Generic timestamp filter processing
"""
def process_filter_data(request):
        
    err_msg = ''
    
    """ Parse the filter fields """
    try:
        
        latest_utc_ts_var = MySQLdb._mysql.escape_string(request.POST['latest_utc_ts'])
        earliest_utc_ts_var = MySQLdb._mysql.escape_string(request.POST['earliest_utc_ts'])
        
        if not TP.is_timestamp(earliest_utc_ts_var, 1) or  not TP.is_timestamp(earliest_utc_ts_var, 1):
            raise TypeError
            
            
        if latest_utc_ts_var == '':
            latest_utc_ts_var = _end_time_
            
    except KeyError:
        
        earliest_utc_ts_var = _beginning_time_
        latest_utc_ts_var = _end_time_
    
    except TypeError:
        
        err_msg = 'Please enter a valid timestamp.'
        
        earliest_utc_ts_var = _beginning_time_
        latest_utc_ts_var = _end_time_

    return err_msg, earliest_utc_ts_var, latest_utc_ts_var


"""
    Display and enable user to 
"""
def mining_patterns_view(request):
    
    banner_patterns, lp_patterns = get_pattern_lists()
            
    return render_to_response('LML/mining_patterns.html', {'banner_patterns' : banner_patterns, 'lp_patterns' : lp_patterns},  context_instance=RequestContext(request))


"""
    Display and enable user to 
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
    banner_patterns, lp_patterns = get_pattern_lists()
    
    return render_to_response('LML/mining_patterns.html', {'err_msg' : err_msg , 'banner_patterns' : banner_patterns, 'lp_patterns' : lp_patterns},  context_instance=RequestContext(request))

        
"""
    Display and enable user to 
"""
def mining_patterns_delete(request):
    
    try:    
        banner_patterns =  request.POST.getlist('banner_patterns')
    
    except:
        banner_patterns = list()
        pass
    
    try:
        lp_patterns =  request.POST.getlist('lp_patterns')
    
    except:        
        lp_patterns = list()
        pass
                
    logging.debug(banner_patterns)
    logging.debug(lp_patterns)
    
    mptl = DL.MiningPatternsTableLoader()
    
    """ Remove selected patterns """
    for elem in banner_patterns:
        mptl.delete_row(pattern=elem,pattern_type='banner')
    
    for elem in lp_patterns:
        mptl.delete_row(pattern=elem,pattern_type='lp')
        
    banner_patterns, lp_patterns = get_pattern_lists()
    
    return render_to_response('LML/mining_patterns.html', {'banner_patterns' : banner_patterns, 'lp_patterns' : lp_patterns},  context_instance=RequestContext(request))


def get_pattern_lists():
    
    mptl = DL.MiningPatternsTableLoader()
    results = mptl.get_all_rows()
    
    banner_patterns = list()
    lp_patterns = list()
    
    for row in results:
        if mptl.get_mining_pattern_field(row, 'pattern_type') == 'banner':
            banner_patterns.append(mptl.get_mining_pattern_field(row, 'pattern'))
        elif mptl.get_mining_pattern_field(row, 'pattern_type') == 'lp':
            lp_patterns.append(mptl.get_mining_pattern_field(row, 'pattern'))
            
    return banner_patterns, lp_patterns
