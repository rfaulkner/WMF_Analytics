
"""

    This module effectively functions as a Singleton class.
    
    This module contains and organizes query info.  Depends on the contents of ../sql/ where filenames are 
    coupled with query_name parameters
    
     METHODS:
        
            format_query         
            get_query            
            get_key_index
            get_count_index
            get_time_index
            get_campaign_index
            get_banner_index
            get_landing_page_index
            get_metric_index
            get_metric_full_name
       

    QUERIES:
        {report_campaign_metrics_minutely_all, report_banner_metrics_minutely_all, report_LP_metrics_minutely_all} ==> used for live reporting on donations and clicks .. over all campaigns, banners, and LPs
        {report_campaign_metrics_minutely, report_banner_metrics_minutely, report_LP_metrics_minutely}             ==> used for interval reporting on campaign tests
        {report_campaign_metrics_minutely_total}                                                                   ==> reports totals for a camapign -- combines with report_campaign_metrics_minutely
"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "November 28th, 2010"

import classes.TimestampProcessor as TP
import classes.FundraiserDataHandler as FDH
import config.settings as projSet
import classes.Helper as Hlp

import datetime, re, MySQLdb, logging, sys

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

"""
    Process kwyword args for format sting
"""
def process_kwargs(kwargs):
    
    country = '.{2}'
    min_donation = 0
    order_str = ''
    
    for key in kwargs:
        if key == 'country':
            if cmp(kwargs['country'], '') == 0:
                country = '.{2}'
            else:
                try:
                    country = MySQLdb._mysql.escape_string(kwargs['country'].strip())
                except:
                    logging.error('QueryData:process_kwargs -- Could not process country for "%s" ' % str(kwargs['country']).strip())
        
        elif key == 'min_donation':
            try:                
                min_donation = int(MySQLdb._mysql.escape_string(str(kwargs['min_donation']).strip())) # ensure that it is an integer
                min_donation = min_donation.__str__() # recast as string
            except:
                logging.error('QueryData:process_kwargs -- Could not process minimum donation for "%s" ' % str(kwargs['min_donation']).strip())
        
        elif key == 'order_str':
            try:
                order_str = MySQLdb._mysql.escape_string(kwargs['order_str'])
            except:
                logging.error('QueryData:process_kwargs -- Could not process order string for "%s" ' % str(str(kwargs['order_str'])))
                
    return country, min_donation, order_str

"""
    Format a saved query to be executed
"""
def format_query(query_name, sql_stmnt, args, **kwargs):
    
    country, min_donation, order_str = process_kwargs(kwargs)
    
    if cmp(query_name, 'report_campaign_ecomm') == 0:
        start_time = args[0]
        sql_stmnt = str(sql_stmnt % (start_time))

    elif cmp(query_name, 'report_campaign_logs') == 0:
        start_time = args[0]
        sql_stmnt = str(sql_stmnt % (start_time, start_time, start_time))

    elif cmp(query_name, 'report_campaign_ecomm_by_hr') == 0:
        start_time = args[0]
        sql_stmnt = str(sql_stmnt % ('%', '%', '%', '%', start_time))

    elif cmp(query_name, 'report_campaign_logs_by_hr') == 0:
        start_time = args[0]
        sql_stmnt = str(sql_stmnt % ('%', '%', '%', '%', start_time, '%', '%', '%', '%', \
        start_time, '%', '%', '%', '%', start_time, '%'))

    elif cmp(query_name, 'report_impressions_country') == 0:
        start_time = args[0]
        sql_stmnt = str(sql_stmnt % ('%', '%', '%', start_time))

    elif cmp(query_name, 'report_campaign_logs_by_min') == 0:
        start_time = args[0]
        sql_stmnt = str(sql_stmnt % ('%', '%', '%', '%', start_time, '%', '%', '%', '%', \
        start_time, '%', '%', '%', '%', start_time))
    
    elif cmp(query_name, 'report_non_US_clicks') == 0:
        start_time = args[0]
        sql_stmnt = str(sql_stmnt % ('%', '%', '%', start_time, '%', '%', '%', start_time))
    
    elif cmp(query_name, 'report_contribution_tracking') == 0:
        start_time = args[0]
        sql_stmnt = str(sql_stmnt % ('%', '%', '%', '%', '%',start_time))
    
    elif cmp(query_name, 'report_total_amounts_by_hr') == 0:
        start_time = args[0]
        end_time = args[1]            
        sql_stmnt = str(sql_stmnt % ('%', '%', '%', ' %H', start_time, end_time))
    
    elif cmp(query_name, 'report_total_amounts_by_day') == 0:
        start_time = args[0]
        end_time = args[1]
        sql_stmnt = str(sql_stmnt % ('%', '%', '%', '', start_time, end_time))
    
    elif cmp(query_name, 'report_LP_metrics') == 0 or cmp(query_name, 'report_LP_metrics_1S') == 0:
        
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        min_views = args[3]
        
        """ Format the condition for minimum views """
        if cmp(str(min_views), '-1') == 0:
            min_views = ' '
        else:
            min_views = 'where lp.views > ' + str(min_views) + ' '
            
        sql_stmnt = str(sql_stmnt % (start_time, end_time, campaign, country, start_time, end_time, campaign, country, start_time, end_time, campaign, country, min_views))
        
    elif cmp(query_name, 'report_banner_metrics') == 0 or cmp(query_name, 'report_bannerLP_metrics') == 0 or cmp(query_name, 'report_total_metrics') == 0 or \
    cmp(query_name, 'report_banner_metrics_1S') == 0 or cmp(query_name, 'report_bannerLP_metrics_1S') == 0 or cmp(query_name, 'report_total_metrics_1S') == 0:
        
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        min_views = args[3]
            
        """ Format the condition for minimum views """
        if cmp(str(min_views), '-1') == 0:
            min_views = ' '
        else:
            min_views = 'where lp.views > ' + str(min_views) + ' '
            
        sql_stmnt = str(sql_stmnt % (start_time, end_time, country, start_time, end_time, campaign, country, start_time, end_time, country, \
                                     start_time, end_time, campaign, country, start_time, end_time, campaign, country, min_views))
        
    
    elif cmp(query_name, 'report_latest_campaign') == 0:
        start_time = args[0]
        sql_stmnt = str(sql_stmnt % (start_time))
            
    elif cmp(query_name, 'report_banner_impressions_by_hour') == 0:
        start = args[0]
        end = args[1]
        sql_stmnt = str(sql_stmnt % ('%','%','%','%', start, end))
                
    elif cmp(query_name, 'report_ecomm_by_amount') == 0:
        start_time = args[0]
        end_time = args[1]
        sql_stmnt = str(sql_stmnt % ('%', '%',  '%',  '%', start_time, end_time, end_time))
    
    elif cmp(query_name, 'report_ecomm_by_contact') == 0:
        where_str = args[0]
        sql_stmnt = str(sql_stmnt % ('%', '%', '%', '%', where_str))
    
    elif cmp(query_name, 'report_LP_metrics_minutely') == 0 or cmp(query_name, 'report_LP_metrics_minutely_1S') == 0:
        
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        interval = args[3]
        
        """ The start time for the impression portion of the query should be one second less"""
        start_time_obj = TP.timestamp_to_obj(start_time,1)
        imp_start_time_obj = start_time_obj + datetime.timedelta(seconds=-1)
        imp_start_time_obj_str = TP.timestamp_from_obj(imp_start_time_obj, 1, 3)
        
        sql_stmnt = str(sql_stmnt % ('%', '%',  '%',  '%', interval, interval, start_time, end_time, campaign, country, '%', '%',  '%',  '%', interval, interval, start_time, end_time, campaign, country, \
                                 start_time, end_time, campaign, country, campaign))
    
    elif cmp(query_name, 'report_banner_metrics_minutely') == 0 or cmp(query_name, 'report_bannerLP_metrics_minutely') == 0 or cmp(query_name, 'report_banner_metrics_minutely_1S') == 0 or cmp(query_name, 'report_bannerLP_metrics_minutely_1S') == 0:
    
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        interval = args[3]
        
        """ The start time for the impression portion of the query should be one second less"""
        start_time_obj = TP.timestamp_to_obj(start_time,1)
        imp_start_time_obj = start_time_obj + datetime.timedelta(seconds=-1)
        imp_start_time_obj_str = TP.timestamp_from_obj(imp_start_time_obj, 1, 3)
        
        sql_stmnt = str(sql_stmnt % ('%', '%', '%',  '%', interval, interval, imp_start_time_obj_str, end_time, \
                                     country, '%', '%',  '%',  '%', interval, interval, start_time, end_time, campaign, country, \
                                '%', '%',  '%',  '%', interval, interval, start_time, end_time, country, \
                                '%', '%',  '%',  '%', interval, interval, start_time, end_time, campaign, \
                                country, start_time, end_time, campaign, country, campaign, ))
    
    elif cmp(query_name, 'report_campaign_metrics_minutely') == 0 or cmp(query_name, 'report_campaign_metrics_minutely_1S') == 0 or cmp(query_name, 'report_campaign_metrics_minutely_total') == 0 \
    or cmp(query_name, 'report_campaign_metrics_minutely_total_1S') == 0:
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        interval = args[3]
        
        sql_stmnt = str(sql_stmnt % (campaign, '%', '%', '%',  '%', interval, interval, start_time, end_time, campaign, country, '%', '%',  '%',  '%', interval, interval, start_time, end_time, campaign, country))
        
    elif cmp(query_name, 'report_campaign_totals') == 0:
        start_time = args[0]
        end_time = args[1]
        
        sql_stmnt = str(sql_stmnt % (start_time, end_time))
    
    elif cmp(query_name, 'report_campaign_banners') == 0:
        start_time = args[0]
        end_time = args[1]
        utm_campaign = args[2]
        
        sql_stmnt = str(sql_stmnt % (start_time, end_time, utm_campaign))
        
    elif cmp(query_name, 'report_campaign_lps') == 0:
        start_time = args[0]
        end_time = args[1]
        utm_campaign = args[2]
        
        sql_stmnt = str(sql_stmnt % (start_time, end_time, utm_campaign))
    
    elif cmp(query_name, 'report_campaign_bannerlps') == 0:
        start_time = args[0]
        end_time = args[1]
        utm_campaign = args[2]
        
        sql_stmnt = str(sql_stmnt % (start_time, end_time, utm_campaign))
    
    elif cmp(query_name, 'report_campaign_metrics_minutely_all') == 0 or cmp(query_name, 'report_banner_metrics_minutely_all') == 0 or cmp(query_name, 'report_lp_metrics_minutely_all') == 0:
        start_time = args[0]
        end_time = args[1]
        interval = args[3]
        
        sql_stmnt = str(sql_stmnt % ('%', '%', '%',  '%', interval, interval, start_time, end_time))

    elif cmp(query_name, 'report_donation_metrics') == 0:
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        
        sql_stmnt = str(sql_stmnt % (start_time, end_time, campaign, country, start_time, end_time, campaign, country))

    elif cmp(query_name, 'report_total_donations') == 0:
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        
        """ Recursively construct the sub-query """
        sub_query_name = 'report_donation_metrics'
        sub_query_sql = Hlp.file_to_string(projSet.__sql_home__ + sub_query_name + '.sql')
        sub_query_sql = format_query(sub_query_name, sub_query_sql, [start_time, end_time, campaign], country=country)        

        sql_stmnt = str(sql_stmnt % sub_query_sql)

    elif cmp(query_name, 'report_daily_totals_by_country') == 0:
        start_time = args[0]
        end_time = args[1]
        
        sql_stmnt = str(sql_stmnt % ('%', '%', '%', start_time, end_time, country, min_donation, order_str))


    else:
        return 'no such table\n'

    return sql_stmnt

"""     
    Returns the index of the key for the query data 
"""
def get_key_index(query_name):
    
    if re.search('report_banner_metrics_minutely', query_name):
        return 1
    elif re.search('report_bannerLP_metrics_minutely', query_name):
        return 1
    elif re.search('report_LP_metrics_minutely', query_name):
        return 1
    elif re.search('report_campaign_metrics_minutely', query_name):
        return 1
    elif query_name == 'report_campaign_metrics_minutely_total':
        return 1
    elif query_name == 'report_campaign_totals':
        return 1
    elif query_name == 'report_campaign_banners':
        return 0
    elif query_name == 'report_campaign_lps':
        return 0
    elif query_name == 'report_campaign_bannerlps':
        return [0, 1, 2]
    elif query_name == 'report_campaign_metrics_minutely_all' or query_name == 'report_banner_metrics_minutely_all' or query_name == 'report_lp_metrics_minutely_all':
        return 1
    elif query_name == 'report_banner_metrics' or query_name == 'report_LP_metrics' or query_name == 'report_bannerLP_metrics' or query_name == 'report_total_metrics' \
    or query_name == 'report_banner_metrics_1S' or query_name == 'report_LP_metrics_1S' or query_name == 'report_bannerLP_metrics_1S' or query_name == 'report_total_metrics_1S':
        return 0
    elif query_name == 'report_bannerLP_metrics' or query_name == 'report_bannerLP_metrics_1S' or query_name == 'report_donation_metrics':
        return [0, 1, 2]
    elif query_name == 'report_total_donations':
        return 0
    else:
        return 1

"""     
    Returns the key value of a given query
"""
def get_key_label(query_name, row):
    
    if cmp(query_name,'report_bannerLP_metrics') == 0 or cmp(query_name,'report_bannerLP_metrics_1S') == 0 or cmp(query_name,'report_donation_metrics') == 0:
        return row[0] + '-' + row[1] + '-' + row[2]
    
    elif cmp(query_name,'report_campaign_bannerlps') == 0:
        return row[0] + '-' + row[1] + '-' + row[2]
    
    else:
        index = get_key_index(query_name)
        return row[index]

#    elif cmp(query_type,'report_summary_results') == 0 or cmp(query_type,'report_summary_results_1S') == 0 or cmp(query_type,'report_summary_country_results') == 0 or cmp(query_type,'report_summary_results_country_1S') == 0:
#        return row[0] + '-' + row[1] + '-' + row[2]
    
    
def get_metric_index(query_name, metric_name):
    
    if query_name == 'report_campaign_logs_by_min':
        if metric_name == 'click_rate':
            return 9
    elif query_name == 'report_campaign_logs_by_hr':
        if metric_name == 'click_rate':
            return 8
    elif query_name == 'report_contribution_tracking':
        if metric_name == 'converted_amount':
            return 4 
        
    elif query_name == 'report_LP_metrics_minutely' or query_name == 'report_LP_metrics_minutely_1S':
        if metric_name == 'day_hr' or metric_name == 'ts':
            return 0
        elif metric_name == 'views':
            return 2
        elif metric_name == 'donations':
            return 3
        elif metric_name == 'amount':
            return 4
        elif metric_name == 'amount_normal':
            return 5
        elif metric_name == 'don_per_view':
            return 6
        elif metric_name == 'amt_norm_per_view':
            return 8
        elif metric_name == 'avg_donation':
            return 9
        elif metric_name == 'avg_donation_norm':
            return 10
        else:
            return -1
        
    elif query_name == 'report_banner_metrics_minutely' or query_name == 'report_banner_metrics_minutely_1S':
        if metric_name == 'day_hr' or metric_name == 'ts':
            return 0
        elif metric_name == 'imp' or metric_name == 'impressions':
            return 2
        elif metric_name == 'donations':
            return 4
        elif metric_name == 'amount_normal':
            return 6
        elif metric_name == 'don_per_imp':
            return 8
        elif metric_name == 'amt_norm_per_imp':
            return 10
        elif metric_name == 'click_rate':
            return 7
        elif metric_name == 'avg_donation':
            return 11
        elif metric_name == 'avg_donation_norm':
            return 12
        else:
            return -1
        
    elif re.search('report_campaign_metrics_minutely', query_name) and not(query_name == 'report_campaign_metrics_minutely_all'):
        if metric_name == 'day_hr' or metric_name == 'ts':
            return 0
        elif metric_name == 'donations':
            return 3
        elif metric_name == 'views':
            return 2
        else:
            return -1
    elif query_name == 'report_campaign_metrics_minutely_total':
        if metric_name == 'donations':
            return 3
        elif metric_name == 'views':
            return 2
        else:
            return -1
    elif query_name == 'report_campaign_totals':
        if metric_name == 'donations':
            return 2
        elif metric_name == 'name':
            return 0
        elif metric_name == 'earliest_timestamp':
            return 3
        else:
            return -1
    
    elif query_name == 'report_campaign_metrics_minutely_all' or query_name == 'report_banner_metrics_minutely_all' or query_name == 'report_lp_metrics_minutely_all':
        if metric_name == 'day_hr' or metric_name == 'ts':
            return 0
        elif metric_name == 'donations':
            return 2
        elif metric_name == 'clicks':
            return 3
        else:
            return -1
    
    elif query_name == 'report_bannerLP_metrics_minutely' or query_name == 'report_bannerLP_metrics_minutely_1S':
        if metric_name == 'day_hr' or metric_name == 'ts':
            return 0
        elif metric_name == 'imp' or metric_name == 'impressions':
            return 2
        elif metric_name == 'views':
            return 3
        elif metric_name == 'donations':
            return 4
        elif metric_name == 'amount_normal':
            return 6
        elif metric_name == 'don_per_imp':
            return 8
        elif metric_name == 'amt_norm_per_imp':
            return 10
        elif metric_name == 'don_per_view':
            return 11
        elif metric_name == 'amt_norm_per_view':
            return 13
        elif metric_name == 'click_rate':
            return 7
        elif metric_name == 'avg_donation':
            return 14
        elif metric_name == 'avg_donation_norm':
            return 15
        elif metric_name == 'utm_campaign':
            return 16
        else:
            return -1
    
    elif query_name == 'report_LP_metrics_1S' or query_name == 'report_LP_metrics':
        
        if metric_name == 'don_per_view':
            return 5
        elif metric_name == 'amt_norm_per_view':
            return 7
        else:
            return -1
        
    elif query_name == 'report_banner_metrics_1S' or query_name == 'report_banner_metrics':
        
        if metric_name == 'click_rate':
            return 6
        elif metric_name == 'don_per_imp':
            return 7
        elif metric_name == 'amt_norm_per_imp':
            return 9
        
    elif query_name == 'report_bannerLP_metrics_1S' or query_name == 'report_bannerLP_metrics':
        
        if metric_name == 'click_rate':
            return 8
        elif metric_name == 'don_per_imp':
            return 9
        elif metric_name == 'amt_norm_per_imp':
            return 11
        elif metric_name == 'don_per_view':
            return 12
        elif metric_name == 'amt_norm_per_view':
            return 14
        
    elif query_name == 'report_total_metrics' or query_name == 'report_total_metrics_1S':
        
        if metric_name == 'click_rate':
            return 6
        elif metric_name == 'don_per_imp':
            return 7
        elif metric_name == 'amt_norm_per_imp':
            return 9
        elif metric_name == 'don_per_view':
            return 10
        elif metric_name == 'amt_norm_per_view':
            return 12
    
    elif query_name == 'report_lp_running':
        
        if metric_name == 'utm_campaign':
            return 0
        elif metric_name == 'country':
            return 1
        elif metric_name == 'language':
            return 2
        elif metric_name == 'live_banners':
            return 3
        elif metric_name == 'landing_page':
            return 4
        elif metric_name == 'views':
            return 5
        elif metric_name == 'donations':
            return 6
        elif metric_name == 'amount':
            return 7
    
    elif query_name == 'report_donation_metrics':
        
        if metric_name == 'utm_campaign':
            return 0
        elif metric_name == 'banner':
            return 1
        elif metric_name == 'landing_page':
            return 2
        elif metric_name == 'donations':
            return 3
        elif metric_name == 'amount':
            return 4
        elif metric_name == 'amount_normal':
            return 5
        
    elif query_name == 'report_total_donations':
        
        if metric_name == 'campaign':
            return 0
        elif metric_name == 'donations':
            return 1
        elif metric_name == 'amount':
            return 2
        elif metric_name == 'amount_normal':
            return 3

    elif query_name == 'report_daily_totals_by_country':
        
        if metric_name == 'donations':
            return 2
        elif metric_name == 'amount':
            return 3
        
    else:
        return 'no such table'

    
def get_metric_full_name(metric_name):
    
    if metric_name == 'imp':
        return 'Banner Impressions'
    elif metric_name == 'imp':
        return 'Banner Impressions'
    elif metric_name == 'views':
        return 'Landing Page Views'
    elif metric_name == 'don_per_imp':
        return 'Donations per Impression'
    elif metric_name == 'don_per_view':
        return 'Donations per View'
    elif metric_name == 'amt50_per_imp':
        return 'Amount50 per Impression'
    elif metric_name == 'amt50_per_view':
        return 'Amount50 per View'
    elif metric_name == 'amt_norm_per_imp':
        return 'Amount Normal per Impression'
    elif metric_name == 'amt_norm_per_view':
        return 'Amount Normal per View'
    elif metric_name == 'amount50':
        return 'Amount50'
    elif metric_name == 'amount_normal':
        return 'Amount Normal'
    elif metric_name == 'donations':
        return 'Donations'
    elif metric_name == 'amt_per_imp':
        return 'Amount per Impression'
    elif metric_name == 'amt_per_view':
        return 'Amount per View'
    elif metric_name == 'amount':
        return 'Amount'
    elif metric_name == 'click_rate':
        return 'Banner Click Rate'
    elif metric_name == 'avg_donation':
        return 'Average Donation'
    elif metric_name == 'avg_donation50':
        return 'Average Donation50'
    elif metric_name == 'avg_donation_norm':
        return 'Average Donation Normal'
    elif metric_name == 'utm_campaign':
        return 'Campaign'
    elif metric_name == 'utm_source':
        return 'Banner'
    elif metric_name == 'banner':
        return 'Banner'
    elif metric_name == 'landing_page':
        return 'Landing Page'
    else:
        return'no such metric'


def get_metric_data_type(metric_name, elem):
    
    if metric_name == 'ts' or metric_name == 'day_hr' or metric_name == 'utm_campaign' or metric_name == 'campaign' or metric_name == 'utm_source' or metric_name == 'landing_page':
        return elem
    elif metric_name == 'imp' or metric_name == 'impressions' or metric_name == 'views' or metric_name == 'donations' or metric_name ==  'clicks' or metric_name == 'total_clicks':
        return str(int(elem))
    elif metric_name == 'amt50_per_view' or metric_name == 'amt50_per_imp' or metric_name == 'don_per_view' or metric_name == 'don_per_imp' or metric_name == 'amt_per_imp' or metric_name == 'amt_per_view' or metric_name == 'click_rate' \
    or metric_name == 'amt_norm_per_view' or metric_name == 'amt_norm_per_imp':
        return "%.6f" % elem
    elif metric_name == 'amount' or metric_name == 'amount50' or metric_name == 'amount100' or metric_name == 'avg_donation' or metric_name == 'avg_donation50' or metric_name == 'amount_normal' or metric_name == 'avg_donation_norm':
        return "%.2f" % elem
    else:
        return'no such metric'


"""
    Given a query return the types of each column for aggregation
    
    @param query: string query name
    
    @return: list of column types for aggregation 
"""
def get_column_types(query):
    
    if cmp(query, 'report_total_metrics') == 0:
                
        return [FDH._COLTYPE_KEY_, FDH._COLTYPE_AMOUNT_, FDH._COLTYPE_AMOUNT_, FDH._COLTYPE_AMOUNT_, FDH._COLTYPE_AMOUNT_, FDH._COLTYPE_AMOUNT_, FDH._COLTYPE_RATE_, \
                FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_]
