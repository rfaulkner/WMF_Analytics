"""

This module defines functionality that producing serialized caching objects for commonly run queries.  The classes in this family
are associated with views to store caching objects for requests from the front end via those views.

"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "November 17th, 2011"


""" Import python base modules """
import sys, datetime, logging, shelve

""" Import Analytics modules """
import config.settings as projSet
import classes.DataReporting as DR
import classes.DataLoader as DL
import classes.FundraiserDataHandler as FDH

import classes.TimestampProcessor as TP
import classes.Helper as Hlp

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

"""

    BASE CLASS :: DataCaching
    
    Defines the basic functionality of a caching class - storing and retrieving data from a serialized object
                   
"""
class DataCaching(object):
    
    DATA_DIR = projSet.__data_file_dir__
         
    def cache_data(self, data, key):
        self._serialized_obj_[key] = data
                    
    def retrieve_cached_data(self, key):
        return self._serialized_obj_[key]
    
"""
    LTT_DataCaching
    
    DataCaching for the long term trends view
    
"""
class LTT_DataCaching(DataCaching):
    
    CACHING_HOME = projSet.__data_file_dir__ + 'ltt_vars.s'
    VIEW_DURATION_HRS = projSet.__HRS_BACK_LTT__
    
    def __init__(self):        
        self.open_serialized_obj()

    
    """ Close the connection to the serialized obj """
    def __del__(self):
        self._serialized_obj_.close()

    def open_serialized_obj(self, **kwargs):        
        self._serialized_obj_ = shelve.open(self.CACHING_HOME)
        
    """
        Executes the processing of data for the long term trends view in live results
    """
    def execute_process(self, key, **kwargs):
        
        logging.info('Commencing caching of long term trends data at:  %s' % self.CACHING_HOME)
                
        end_time, start_time = TP.timestamps_for_interval(datetime.datetime.utcnow() + datetime.timedelta(minutes=-20), 1, hours=-self.VIEW_DURATION_HRS)
        
        """
            DATA CONFIG            
        """
        
        countries = DL.CiviCRMLoader().get_ranked_donor_countries(start_time)
        countries = countries[1:6]
        
        """ set the metrics to plot """
        lttdl = DL.LongTermTrendsLoader(db='storage3')
        metrics = ['impressions', 'views', 'donations', 'donations', 'amount', 'click_rate', 'amount']
        metrics_index = [0, 1, 2, 2, 2, 3, 4]
        
        """ Dictionary object storing lists of regexes - each expression must pass for a label to persist """
        country_groups = {'US': ['(US)'], 'CA': ['(CA)'], 'JP': ['(JP)'], 'IN': ['(IN)'], 'NL': ['(NL)']}
        currency_groups = {'USD' : ['(USD)'], 'CAD': ['(CAD)'], 'JPY': ['(JPY)'], 'EUR': ['(EUR)']}
        lang_cntry_groups = {'US': ['US..', '.{4}'], 'EN' : ['[^U^S]en', '.{4}']}
        
        top_cntry_groups = dict()
        for country in countries:
            top_cntry_groups[country] = ["'" + country + "'", '.{2}']
        
        groups = [lang_cntry_groups, lang_cntry_groups, lang_cntry_groups, top_cntry_groups, lang_cntry_groups, country_groups, currency_groups]
        
        """  The metrics that are used to build a group string to be qualified via regex - the values of the list metrics are concatenated """ 
        group_metrics = [['country', 'language'], ['country', 'language'], ['country', 'language'], ['country'], \
                         ['country', 'language'], ['country', 'language'], ['currency']]
        
        metric_types = [DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_AMOUNT_, \
                        DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_AMOUNT_, \
                        DL.LongTermTrendsLoader._MT_RATE_, DL.LongTermTrendsLoader._MT_AMOUNT_]
        
        include_totals = [True, True, True, False, True, True, True]
        include_others = [True, True, True, False, True, True, True]
        
        data = list()
        
        """ END CONFIG """
        
        
        """ For each metric use the LongTermTrendsLoader to generate the data to plot """
        for index in range(len(metrics)):
            
            dr = DR.DataReporting()
            
            times, counts = lttdl.run_query(start_time, end_time, metrics_index[index], metric_name=metrics[index], metric_type=metric_types[index], \
                                            groups=groups[index], group_metric=group_metrics[index], include_other=include_others[index], include_total=include_totals[index])
            times = TP.normalize_timestamps(times, False, 1)
                
            dr._counts_ = counts
            dr._times_ = times
    
            empty_data = [0] * len(times['Total'])
            data.append(dr.get_data_lists([''], empty_data))
        
        dict_param = Hlp.combine_data_lists(data)
        dict_param['interval'] = self.VIEW_DURATION_HRS    
        dict_param['end_time'] = TP.timestamp_convert_format(end_time,1,2)
        
        self.cache_data(dict_param, key)
        
        logging.info('Caching complete.')
        

"""
    LiveResults_DataCaching
    
    DataCaching for the long term trends view
    
"""
class LiveResults_DataCaching(DataCaching):
    
    CACHING_HOME = projSet.__data_file_dir__ + 'live_results_vars.s'
    DURATION_HRS = projSet.__HRS_BACK_LIVE_RESULTS__
    
    def __init__(self):        
        self.open_serialized_obj()
        
    
    """ Close the connection to the serialized obj """
    def __del__(self):
        self._serialized_obj_.close()

    def open_serialized_obj(self, **kwargs):        
        self._serialized_obj_ = shelve.open(self.CACHING_HOME)
        
    """
        Executes the processing of data for the long term trends view in live results
    """
    def execute_process(self, key, **kwargs):
        
        logging.info('Commencing caching of live results data at:  %s' % self.CACHING_HOME)
        shelve_key = key
        
        """ Find the earliest and latest page views for a given campaign  """
        lptl = DL.LandingPageTableLoader(db='storage3')
            
        query_name = 'report_summary_results.sql'
        query_name_1S = 'report_summary_results_1S.sql'                    
        campaign_regexp_filter = '^C_|^C11_'
                
        dl = DL.DataLoader(db='storage3')
        end_time, start_time = TP.timestamps_for_interval(datetime.datetime.utcnow(), 1, hours=-self.DURATION_HRS)
        
        """ Should a one-step query be used? """        
        use_one_step = lptl.is_one_step(start_time, end_time, 'C11')  # Assume it is a one step test if there are no impressions for this campaign in the landing page table
        
        """ 
            Retrieve the latest time for which impressions have been loaded
            ===============================================================
        """
        
        sql_stmnt = 'select max(end_time) as latest_ts from squid_log_record where log_completion_pct = 100.00'
        
        results = dl.execute_SQL(sql_stmnt)
        latest_timestamp = results[0][0]
        latest_timestamp = TP.timestamp_from_obj(latest_timestamp, 2, 3)
        latest_timestamp_flat = TP.timestamp_convert_format(latest_timestamp, 2, 1)
    
        ret = DR.ConfidenceReporting(query_type='', hyp_test='', db='storage3').get_confidence_on_time_range(start_time, end_time, campaign_regexp_filter, one_step=use_one_step)
        measured_metrics_counts = ret[1]
        
        """ Prepare Summary results """
        
        sql_stmnt = Hlp.file_to_string(projSet.__sql_home__ + query_name)
        sql_stmnt = sql_stmnt % (start_time, latest_timestamp_flat, start_time, latest_timestamp_flat, campaign_regexp_filter, start_time, latest_timestamp_flat, \
                                 start_time, end_time, campaign_regexp_filter, start_time, end_time, campaign_regexp_filter, start_time, end_time, campaign_regexp_filter, \
                                 start_time, latest_timestamp_flat, campaign_regexp_filter, start_time, latest_timestamp_flat, campaign_regexp_filter)        
        
        logging.info('Executing report_summary_results ...')
        
        results = dl.execute_SQL(sql_stmnt)
        column_names = dl.get_column_names()
        
        if use_one_step:
            
            logging.info('... including one step artifacts ...')
            
            sql_stmnt_1S = Hlp.file_to_string(projSet.__sql_home__ + query_name_1S)
            sql_stmnt_1S = sql_stmnt_1S % (start_time, latest_timestamp_flat, start_time, latest_timestamp_flat, campaign_regexp_filter, start_time, latest_timestamp_flat, \
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
            
        metric_legend_table = DR.DataReporting().get_standard_metrics_legend()
        conf_legend_table = DR.ConfidenceReporting(query_type='bannerlp', hyp_test='TTest').get_confidence_legend_table()

        """ Create a interval loader objects """
        
        sampling_interval = 5 # 5 minute sampling interval for donation plots
        
        ir_cmpgn = DR.IntervalReporting(query_type=FDH._QTYPE_CAMPAIGN_ + FDH._QTYPE_TIME_, generate_plot=False, db='storage3')
        ir_banner = DR.IntervalReporting(query_type=FDH._QTYPE_BANNER_ + FDH._QTYPE_TIME_, generate_plot=False, db='storage3')
        ir_lp = DR.IntervalReporting(query_type=FDH._QTYPE_LP_ + FDH._QTYPE_TIME_, generate_plot=False, db='storage3')
            
        """ Execute queries """        
        ir_cmpgn.run(start_time, end_time, sampling_interval, 'donations', '',{})
        ir_banner.run(start_time, end_time, sampling_interval, 'donations', '',{})
        ir_lp.run(start_time, end_time, sampling_interval, 'donations', '',{})
        
        
        """ Prepare serialized objects """
        
        dict_param = dict()

        dict_param['metric_legend_table'] = metric_legend_table
        dict_param['conf_legend_table'] = conf_legend_table
        
        dict_param['measured_metrics_counts'] = measured_metrics_counts
        dict_param['results'] = results
        dict_param['column_names'] = column_names

        dict_param['interval'] = sampling_interval
        dict_param['duration'] = self.DURATION_HRS    
        
        dict_param['start_time'] = TP.timestamp_convert_format(start_time,1,2)
        dict_param['end_time'] = TP.timestamp_convert_format(end_time,1,2)
        
        dict_param['ir_cmpgn_counts'] = ir_cmpgn._counts_
        dict_param['ir_banner_counts'] = ir_banner._counts_
        dict_param['ir_lp_counts'] = ir_lp._counts_
        
        dict_param['ir_cmpgn_times'] = ir_cmpgn._times_
        dict_param['ir_banner_times'] = ir_banner._times_
        dict_param['ir_lp_times'] = ir_lp._times_
        
        self.cache_data(dict_param, shelve_key)
        
        logging.info('Caching complete.')
        