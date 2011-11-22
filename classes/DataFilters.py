"""

    The DataFilters module provides a way to filter data to mutate dictionaries.  Each filter contains an execute( method that 

    
    
"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "July 5th, 2011"

import sys, logging
import classes.TimestampProcessor as TP


""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')
        
"""
    BASE CLASS for filters.  The interface defines the filter method which is called
"""
class DataFilter(object):
    
    """
        Perform initialization boilerplate for all classes
    """
    def __init__(self, **kwargs):
        
        # logging.info('Creating filter ' + self.__str__())
        
        """ The mutable object will contain the data structures on which the filter will operate """
        for key in kwargs:    
            if key == 'mutable_obj':
                self._mutable_obj_ = kwargs[key]
            
    """
        Execution method.  The base class simply performs the logging.
    """
    def execute(self):
        # logging.info(self.__str__() + ' -- filtering...')
        return
    
    
"""
    This filter removes keys from a dictionary whose sum falls under a given value.  It is applied to DataReporting objects.
"""    
class TotalCountFilter(DataFilter):
    
    """
       Define the lower bound of the sum of the data dictionary list entry
    """
    def __init__(self, **kwargs):
        
        self._lower_bound_ = 0
        
        for key in kwargs:
            if key == 'lower_bound':
                self._lower_bound_ = kwargs[key]
                
        DataFilter.__init__(self, **kwargs)
    
    """
        Remove keys whose sum is below _lower_bound_ 
        
        @param params: data dictionary
    """
    def execute(self):
        
        DataFilter.execute(self)
        
        new_counts = dict()
        counts = self._mutable_obj_.get_counts()
        
        for key in counts.keys():
            if sum(counts[key]) > self._lower_bound_:
                new_counts[key] = counts[key]
        
        self._mutable_obj_.set_counts(new_counts)
    
"""
    Provide a list of keys to include in a dictionary
"""    
class MatchKeysDataReporterFilter(DataFilter):
    
    """
        The base constructor supplies all that is needed 
    """
    def __init__(self, **kwargs):       
        
        DataFilter.__init__(self, **kwargs)
 
    """
        Removes all keys not in the key list and adds a default list of times for those keys missing (however this should generally not occur)
    """
    def execute(self):
        
        DataFilter.execute(self)
        
        counts = self._mutable_obj_.get_counts()
        times = self._mutable_obj_.get_times()
        
        new_data_dict = dict()
        
        try:
            """ Retrieve the first list of times to use as a template for missing keys  """
            template_times_list = times[times.keys()[0]]
        except:
            logging.error('The dictionary storing time data is empty.')
            
        for key in counts.keys():
            if key in times.keys():
                new_data_dict[key] = times[key]
            else:
                new_data_dict[key] = template_times_list
        
        self._mutable_obj_.set_times(new_data_dict)

"""
    This filter removes keys from a dictionary whose sum falls under a given value.  It is applied to DataReporting objects.
"""    
class TimeSeriesDataFilter(DataFilter):
    
    """
        The base constructor supplies all that is needed 
    """
    def __init__(self, **kwargs):       
        
        DataFilter.__init__(self, **kwargs)
        
        self._interval_ = 1
        self._artifact_keys_ = list()
        
        for key in kwargs:    
            if key == 'artifact_keys':
                self._artifact_keys_ = kwargs[key]
            if key == 'interval':
                self._interval_ = kwargs[key]
                
    """
        Removes all keys not in the key list and adds a default list of times for those keys missing (however this should generally not occur)
    """
    def execute(self):
        
        DataFilter.execute(self)
        
        counts = self._mutable_obj_._counts_
        times = self._mutable_obj_._times_
        item_keys = self._mutable_obj_._item_keys_

        """ Select only the specified item keys """
        if len(item_keys) > 0:
            counts = self._mutable_obj_.select_metric_keys(counts)
            times = self._mutable_obj_.select_metric_keys(times)
        
        """ Convert Times to Integers that indicate relative times AND normalize the intervals in case any are missing """
        for key in times.keys():
            times[key] = TP.normalize_timestamps(times[key], False, 3)        

            """ BUG FIX - remove any repeated indices """
            fixed_times = list()
            for time_index in range(len(times[key]) - 1):
                if times[key][time_index] != times[key][time_index + 1]:
                    fixed_times.append(times[key][time_index])
            if len(times[key]) > 0:
                fixed_times.append(times[key][-1])
            times[key] = fixed_times
            times[key], counts[key] = TP.normalize_intervals(times[key], counts[key], self._interval_)                    
            
        """ If there are missing metrics add them as zeros """
        for artifact_key in self._artifact_keys_:

            if not(artifact_key in times.keys()):
                times[artifact_key] = times[times.keys()[0]]
                counts[artifact_key] = [0.0] * len(times[artifact_key])
        
        """  Remove artifacts not in the list if there are any labels specified """
        if len(self._artifact_keys_) > 0:
            for key in counts.keys():
                if key not in self._artifact_keys_:
                    del counts[key]
                    del times[key]
                    