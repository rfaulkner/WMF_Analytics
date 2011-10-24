
"""

    This module effectively functions as a Singleton class.
    
    This module contains and organizes query info.  Depends on the contents of ../sql/ where filenames are 
    coupled with query_name parameters
    
     METHODS:
        
            format_query         
            get_query             
            get_query_header     
            get_key_index
            get_count_index
            get_time_index
            get_campaign_index
            get_banner_index
            get_landing_page_index
            get_metric_index
            get_plot_title
            get_plot_ylabel
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
import datetime, re

def format_query(query_name, sql_stmnt, args):
    
    if cmp(query_name, 'report_campaign_ecomm') == 0:
        start_time = args[0]
        sql_stmnt = sql_stmnt % (start_time)

    elif cmp(query_name, 'report_campaign_logs') == 0:
        start_time = args[0]
        sql_stmnt = sql_stmnt % (start_time, start_time, start_time)

    elif cmp(query_name, 'report_campaign_ecomm_by_hr') == 0:
        start_time = args[0]
        sql_stmnt = sql_stmnt % ('%', '%', '%', '%', start_time)

    elif cmp(query_name, 'report_campaign_logs_by_hr') == 0:
        start_time = args[0]
        sql_stmnt = sql_stmnt % ('%', '%', '%', '%', start_time, '%', '%', '%', '%', \
        start_time, '%', '%', '%', '%', start_time, '%')

    elif cmp(query_name, 'report_impressions_country') == 0:
        start_time = args[0]
        sql_stmnt = sql_stmnt % ('%', '%', '%', start_time)

    elif cmp(query_name, 'report_campaign_logs_by_min') == 0:
        start_time = args[0]
        sql_stmnt = sql_stmnt % ('%', '%', '%', '%', start_time, '%', '%', '%', '%', \
        start_time, '%', '%', '%', '%', start_time)
    
    elif cmp(query_name, 'report_non_US_clicks') == 0:
        start_time = args[0]
        sql_stmnt = sql_stmnt % ('%', '%', '%', start_time, '%', '%', '%', start_time)
    
    elif cmp(query_name, 'report_contribution_tracking') == 0:
        start_time = args[0]
        sql_stmnt = sql_stmnt % ('%', '%', '%', '%', '%',start_time)
    
    elif cmp(query_name, 'report_total_amounts_by_hr') == 0:
        start_time = args[0]
        end_time = args[1]            
        sql_stmnt = sql_stmnt % ('%', '%', '%', ' %H', start_time, end_time)
    
    elif cmp(query_name, 'report_total_amounts_by_day') == 0:
        start_time = args[0]
        end_time = args[1]
        sql_stmnt = sql_stmnt % ('%', '%', '%', '', start_time, end_time)
    
    elif cmp(query_name, 'report_LP_metrics') == 0 or cmp(query_name, 'report_LP_metrics_1S') == 0:
        
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        min_views = args[3]
        
        sql_stmnt = sql_stmnt % (start_time, end_time, campaign, start_time, end_time, campaign, campaign, min_views)
        
    elif cmp(query_name, 'report_banner_metrics') == 0 or cmp(query_name, 'report_bannerLP_metrics') == 0 or cmp(query_name, 'report_total_metrics') == 0 or \
    cmp(query_name, 'report_banner_metrics_1S') == 0 or cmp(query_name, 'report_bannerLP_metrics_1S') == 0:
        
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        min_views = args[3]
        
        sql_stmnt = sql_stmnt % (start_time, end_time, start_time, end_time, campaign, start_time, end_time, start_time, end_time, campaign, campaign, min_views)
    
    elif cmp(query_name, 'report_latest_campaign') == 0:
        start_time = args[0]
        sql_stmnt = sql_stmnt % (start_time)
            
    elif cmp(query_name, 'report_banner_impressions_by_hour') == 0:
        start = args[0]
        end = args[1]
        sql_stmnt = sql_stmnt % ('%','%','%','%', start, end)
                
    elif cmp(query_name, 'report_ecomm_by_amount') == 0:
        start_time = args[0]
        end_time = args[1]
        sql_stmnt = sql_stmnt % ('%', '%',  '%',  '%', start_time, end_time, end_time)
    
    elif cmp(query_name, 'report_ecomm_by_contact') == 0:
        where_str = args[0]
        sql_stmnt = sql_stmnt % ('%', '%', '%', '%', where_str)
    
    elif cmp(query_name, 'report_LP_metrics_minutely') == 0 or cmp(query_name, 'report_LP_metrics_minutely_1S') == 0:
        
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        interval = args[3]
        
        """ The start time for the impression portion of the query should be one second less"""
        start_time_obj = TP.timestamp_to_obj(start_time,1)
        imp_start_time_obj = start_time_obj + datetime.timedelta(seconds=-1)
        imp_start_time_obj_str = TP.timestamp_from_obj(imp_start_time_obj, 1, 3)
        
        sql_stmnt = sql_stmnt % ('%', '%',  '%',  '%', interval, interval, start_time, end_time, campaign, '%', '%',  '%',  '%', interval, interval, start_time, end_time, campaign, campaign)
    
    elif cmp(query_name, 'report_banner_metrics_minutely') == 0 or cmp(query_name, 'report_bannerLP_metrics_minutely') == 0 or cmp(query_name, 'report_banner_metrics_minutely_1S') == 0 or cmp(query_name, 'report_bannerLP_metrics_minutely_1S') == 0:
    
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        interval = args[3]
        
        """ The start time for the impression portion of the query should be one second less"""
        start_time_obj = TP.timestamp_to_obj(start_time,1)
        imp_start_time_obj = start_time_obj + datetime.timedelta(seconds=-1)
        imp_start_time_obj_str = TP.timestamp_from_obj(imp_start_time_obj, 1, 3)
        
        sql_stmnt = sql_stmnt % ('%', '%', '%',  '%', interval, interval, imp_start_time_obj_str, end_time, '%', '%',  '%',  '%', interval, interval, start_time, end_time, campaign, \
                                '%', '%',  '%',  '%', interval, interval, start_time, end_time, '%', '%',  '%',  '%', interval, interval, start_time, end_time, campaign, campaign)
    
    elif cmp(query_name, 'report_campaign_metrics_minutely') == 0 or cmp(query_name, 'report_campaign_metrics_minutely_1S') == 0 or cmp(query_name, 'report_campaign_metrics_minutely_total') == 0 \
    or cmp(query_name, 'report_campaign_metrics_minutely_total_1S') == 0:
        start_time = args[0]
        end_time = args[1]
        campaign = args[2]
        interval = args[3]
        
        sql_stmnt = sql_stmnt % (campaign, '%', '%', '%',  '%', interval, interval, start_time, end_time, campaign, '%', '%',  '%',  '%', interval, interval, start_time, end_time, campaign)
    
    elif cmp(query_name, 'report_campaign_totals') == 0:
        start_time = args[0]
        end_time = args[1]
        
        sql_stmnt = sql_stmnt % (start_time, end_time)
    
    elif cmp(query_name, 'report_campaign_banners') == 0:
        start_time = args[0]
        end_time = args[1]
        utm_campaign = args[2]
        
        sql_stmnt = sql_stmnt % (start_time, end_time, utm_campaign)
        
    elif cmp(query_name, 'report_campaign_lps') == 0:
        start_time = args[0]
        end_time = args[1]
        utm_campaign = args[2]
        
        sql_stmnt = sql_stmnt % (start_time, end_time, utm_campaign)
    
    elif cmp(query_name, 'report_campaign_bannerlps') == 0:
        start_time = args[0]
        end_time = args[1]
        utm_campaign = args[2]
        
        sql_stmnt = sql_stmnt % (start_time, end_time, utm_campaign)
    
    elif cmp(query_name, 'report_campaign_metrics_minutely_all') == 0 or cmp(query_name, 'report_banner_metrics_minutely_all') == 0 or cmp(query_name, 'report_lp_metrics_minutely_all') == 0:
        start_time = args[0]
        end_time = args[1]
        interval = args[3]
        
        sql_stmnt = sql_stmnt % ('%', '%', '%',  '%', interval, interval, start_time, end_time)
        
    else:
        return 'no such table\n'

    return sql_stmnt


def get_query_header(query_name):
    if query_name == 'report_contribution_tracking':
        return ['Time','Banner','Landing Page','Campaign','Converted Amount', 'Suffix']
    elif query_name == 'report_ecomm_by_amount':
        return ['Timestamp','First Name','Last Name','Country','ISO Code', 'Amount', 'First Donation?', 'Date of First']
    elif query_name == 'report_ecomm_by_contact':
        return ['Timestamp','First Name','Last Name','Country','ISO Code', 'Amount']
    else:
        return 'no such table'

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
        return [0, 1]
    elif query_name == 'report_campaign_metrics_minutely_all' or query_name == 'report_banner_metrics_minutely_all' or query_name == 'report_lp_metrics_minutely_all':
        return 1
    elif query_name == 'report_banner_metrics' or query_name == 'report_LP_metrics' or query_name == 'report_bannerLP_metrics' or query_name == 'report_total_metrics' \
    or query_name == 'report_banner_metrics_1S' or query_name == 'report_LP_metrics_1S' or query_name == 'report_bannerLP_metrics_1S' or query_name == 'report_total_metrics_1S':
        return 0
    else:
        return 1
    
"""     
    Returns the index of the timestamp for the query data 
"""
def get_time_index(query_name):
    if query_name == 'report_campaign_logs_by_min':
        return 0
    elif query_name == 'report_campaign_logs_by_hr':
        return 0
    elif query_name == 'report_non_US_clicks':
        return 0
    elif query_name == 'report_contribution_tracking':
        return 0
    elif query_name == 'report_bannerLP_metrics':
        return 0
    elif query_name == 'report_latest_campaign':
        return 1
    elif query_name == 'report_banner_impressions_by_hour':
        return 0
    elif query_name == 'report_lp_views_by_hour':
        return 0
    elif re.search('report_banner_metrics_minutely', query_name):
        return 0
    elif re.search('report_bannerLP_metrics_minutely', query_name):
        return 0
    elif re.search('report_LP_metrics_minutely', query_name):
        return 0
    elif re.search('report_campaign_metrics_minutely', query_name):
        return 0
    elif query_name == 'report_campaign_metrics_minutely_total':
        return 0
    elif query_name == 'report_campaign_totals':
        return 0
    elif query_name == 'report_campaign_metrics_minutely_all' or query_name == 'report_banner_metrics_minutely_all' or query_name == 'report_lp_metrics_minutely_all':
        return 0
    else:
        return -1



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
        
        if metric_name == 'views':
            return 2
        elif metric_name == 'donations':
            return 3
        elif metric_name == 'amount':
            return 4
        elif metric_name == 'amount50':
            return 5
        elif metric_name == 'don_per_view':
            return 6
        elif metric_name == 'amt50_per_view':
            return 8
        elif metric_name == 'avg_donation':
            return 9
        elif metric_name == 'avg_donation50':
            return 10
        else:
            return -1
        
    elif query_name == 'report_banner_metrics_minutely' or query_name == 'report_banner_metrics_minutely_1S':
        if metric_name == 'imp':
            return 2
        elif metric_name == 'donations':
            return 4
        elif metric_name == 'amount50':
            return 6
        elif metric_name == 'don_per_imp':
            return 8
        elif metric_name == 'amt50_per_imp':
            return 10
        elif metric_name == 'click_rate':
            return 7
        elif metric_name == 'avg_donation':
            return 11
        elif metric_name == 'avg_donation50':
            return 12
        else:
            return -1
        
    elif re.search('report_campaign_metrics_minutely', query_name):
        if metric_name == 'donations':
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
        if metric_name == 'donations':
            return 2
        elif metric_name == 'clicks':
            return 3
        else:
            return -1
    
    elif query_name == 'report_bannerLP_metrics_minutely' or query_name == 'report_bannerLP_metrics_minutely_1S':
        if metric_name == 'imp':
            return 2
        elif metric_name == 'views':
            return 3
        elif metric_name == 'donations':
            return 4
        elif metric_name == 'amount50':
            return 6
        elif metric_name == 'don_per_imp':
            return 8
        elif metric_name == 'amt50_per_imp':
            return 10
        elif metric_name == 'don_per_view':
            return 11
        elif metric_name == 'amt50_per_view':
            return 13
        elif metric_name == 'click_rate':
            return 7
        elif metric_name == 'avg_donation':
            return 14
        elif metric_name == 'avg_donation50':
            return 15
        else:
            return -1
    
    elif query_name == 'report_LP_metrics_1S' or query_name == 'report_LP_metrics':
        
        if metric_name == 'don_per_view':
            return 5
        elif metric_name == 'amt50_per_view':
            return 7
        else:
            return -1
        
    elif query_name == 'report_banner_metrics_1S' or query_name == 'report_banner_metrics':
        
        if metric_name == 'click_rate':
            return 6
        elif metric_name == 'don_per_imp':
            return 7
        elif metric_name == 'amt50_per_imp':
            return 9
        
    elif query_name == 'report_bannerLP_metrics_1S' or query_name == 'report_bannerLP_metrics':
        
        if metric_name == 'click_rate':
            return 6
        elif metric_name == 'don_per_imp':
            return 7
        elif metric_name == 'amt50_per_imp':
            return 9
        elif metric_name == 'don_per_view':
            return 10
        elif metric_name == 'amt50_per_view':
            return 12
        
    elif query_name == 'report_total_metrics' or query_name == 'report_total_metrics_1S':
        
        if metric_name == 'click_rate':
            return 6
        elif metric_name == 'don_per_imp':
            return 7
        elif metric_name == 'amt50_per_imp':
            return 9
        elif metric_name == 'don_per_view':
            return 10
        elif metric_name == 'amt50_per_view':
            return 12
        
    else:
        return 'no such table'

"""
    Based on a cursor object retrieve the index of a column name in the data object returned
    
    @param cursor: MySQLdb.cursor object
    @param column_name: string column name
    
    @return: index of the column in the data results
"""
def get_columnn_index(cursor, column_name):
    
    query_cols = list()
    for col in cursor.description:
        query_cols.append(col[0])
                        
    return query_cols.index(column_name)
    


def get_plot_title(query_name):
    if query_name == 'report_banner_impressions_by_hour':
        return 'Banner Impressions Over the Past 24 Hours'
    elif query_name == 'report_lp_views_by_hour':
        return 'Landing Page Views Over the Past 24 Hours'
    else:
        return 'no such table'
    
def get_plot_ylabel(query_name):
    if query_name == 'report_banner_impressions_by_hour':
        return 'IMPRESSIONS'
    elif query_name == 'report_lp_views_by_hour':
        return 'VIEWS'
    else:
        return'no such table'
    
def get_metric_full_name(metric_name):
    if metric_name == 'imp':
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
    elif metric_name == 'amount50':
        return 'Amount50'
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
    else:
        return'no such metric'


def get_metric_data_type(metric_name, elem):
    
    if metric_name == 'ts' or metric_name == 'day_hr' or metric_name == 'utm_campaign' or metric_name == 'campaign' or metric_name == 'utm_source' or metric_name == 'landing_page':
        return elem
    elif metric_name == 'imp' or metric_name == 'impressions' or metric_name == 'views' or metric_name == 'donations' or metric_name ==  'clicks' or metric_name == 'total_clicks':
        return str(int(elem))
    elif metric_name == 'amt50_per_view' or metric_name == 'amt50_per_imp' or metric_name == 'don_per_view' or metric_name == 'don_per_imp' or metric_name == 'amt_per_imp' or metric_name == 'amt_per_view' or metric_name == 'click_rate':
        return "%.6f" % elem
    elif metric_name == 'amount' or metric_name == 'amount50' or metric_name == 'amount100' or metric_name == 'avg_donation' or metric_name == 'avg_donation50':
        return "%.2f" % elem
    else:
        return'no such metric'


"""
    Given a qury return the types of each column for aggregation
    
    @param query: string query name
    
    @return: list of column types for aggregation 
"""
def get_columnn_types(query):
    
    if cmp(query, 'report_total_metrics') == 0:
                
        return [FDH._COLTYPE_KEY_, FDH._COLTYPE_AMOUNT_, FDH._COLTYPE_AMOUNT_, FDH._COLTYPE_AMOUNT_, FDH._COLTYPE_AMOUNT_, FDH._COLTYPE_AMOUNT_, FDH._COLTYPE_RATE_, \
                FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_, FDH._COLTYPE_RATE_]
