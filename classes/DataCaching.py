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
    
    def __init__(self):     
        self._serialized_obj_ = None
        
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
    VIEW_DURATION_HRS = 72
    
    def __init__(self):        
        self.open_serialized_obj()
    
    """ Close the connection to the serialized obj """
    def __del__(self):
        self._serialized_obj_.close()

    def open_serialized_obj(self, **kwargs):        
        self._serialized_obj_ = shelve.open(self.CACHING_HOME)
    
    def get_cached_data(self, key):
        return self._serialized_obj_[key]
    
    """
        Executes the processing of data for the long term trends view in live results
    """
    def execute_process(self, key, **kwargs):
        
        logging.info('Commencing caching of long term trends data at:  %s' % self.CACHING_HOME)
        
        self.open_serialized_obj()
        
        end_time, start_time = TP.timestamps_for_interval(datetime.datetime.utcnow() + datetime.timedelta(minutes=-20), 1, hours=-self.VIEW_DURATION_HRS)
        
        """ set the metrics to plot """
        lttdl = DL.LongTermTrendsLoader()
        metrics = ['impressions', 'views', 'donations', 'amount', 'click_rate', 'amount']
        metrics_index = [0, 1, 2, 2, 3, 4]
        
        country_groups = {'US':'(US)', 'CA':'(CA)', 'JP':'(JP)', 'IN':'(IN)', 'NL':'(NL)', 'Other':'(US|CA|JP|IN|NL)'}
        currency_groups = {'USD':'(USD)', 'CAD':'(CAD)', 'JPY':'(JPY)', 'EUR':'(EUR)', 'Other':'(USD|CAD|JPY|EUR)'}
        
        groups = [country_groups, country_groups, country_groups, country_groups, country_groups, currency_groups]
        group_metrics = ['country', 'country', 'country', 'country', 'country', 'currency']
        
        metric_types = [DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_AMOUNT_, DL.LongTermTrendsLoader._MT_RATE_, DL.LongTermTrendsLoader._MT_AMOUNT_]
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
        dict_param['interval'] = self.VIEW_DURATION_HRS    
        
        self.cache_data(dict_param, key)
        
        logging.info('Caching complete.')
        
        
        