"""

This module provides access to the datasource and enables querying.  The class
structure defined has DataLoader as the base which outlines the basic members
and functionality.  This interface is extended for interaction with specific 
sources of data.

These classes are used to define the data sources for the DataReporting family of 
classes in an Adapter structural pattern. 

"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "April 8th, 2011"


""" Import python base modules """
import sys, MySQLdb, math, datetime, re, logging, csv, operator

""" Import Analytics modules """
import Fundraiser_Tools.settings as projSet
import Fundraiser_Tools.classes.QueryData as QD
import Fundraiser_Tools.classes.TimestampProcessor as TP
import Fundraiser_Tools.classes.Helper as Hlp
import Fundraiser_Tools.classes.FundraiserDataHandler as FDH

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

"""

    BASE CLASS :: DataLoader
    
    This is the base class for data handling functionality of metrics.  Inherited classes will be associated with a set of SQL files that loosely define a set related query types.
    
    Handles database connections.  Stores state values based on query results and also meta data for the queries themselves which can be resolved to full statements by interacting with QueryData.
    
    Data is stored according to a key name on which sets are separated.  Subclasses can further specify how the data is split and related.
    
    MEMBERS:
            @var _db_: instance of MySQLdb connection
            @var _cur_: cursor object for a MySQL conne ction

    METHODS:
            init_db         
            close_db     
            compose_key                    - build a new key in the results based on existing one
            include_keys                   - include only certain keys based on key name
            exclude_keys                   - explicitly remove certain types of keys based on key name 
            get_sql_filename_for_query     - resolve a sql filename based on the simpler query type   
            
               
"""
class DataLoader(object):
 
    _query_names_ = dict()
    _data_handler_ = None   # class that will define how to process the query fields
    _query_type_ = ''       # Stores the query type (dependent on the data handler definition)
    _results_ = None
    _col_names_ = None
    _was_run_ = False
    
    """
        Constructor
        
        Ensure that the query is run for each new object
    """
    def __init__(self):
        
        """ Database and Cursor objects """
        #_db_ = None
        #_cur_ = None
    
        """ State for all new dataloader objects is set to indicate that the associated SQL has yet to be run """
        self._was_run_ = False
        
        
    def init_db(self):
        
        """ Establish connection """
        #self._db_ = MySQLdb.connect(host=projSet.__db_server__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__, passwd=projSet.__pass__)
        self._db_ = MySQLdb.connect(host=projSet.__db_server__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__)
        
        """ Create cursor """
        self._cur_ = self._db_.cursor()
    
    def close_db(self):
        self._cur_.close()
        self._db_.close()
        
    def establish_faulkner_conn(self):
        
        self.close_db()
        self._db_ = MySQLdb.connect(host=projSet.__db_server__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__)
        self._cur_ = self._db_.cursor()
    
    def establish_enwiki_conn(self):
        
        self.close_db()
        self._db_ = MySQLdb.connect(host=projSet.__db_server_internproxy__, user=projSet.__user_internproxy__, db=projSet.__db_internproxy__, port=projSet.__db_port_internproxy__, passwd=projSet.__pass_internproxy__)
        self._cur_ = self._db_.cursor()
    
    
    """
        Make a new key-entry based on the search key and action.  Take all keys containing the search_key
        and compose a new key with action.
        
        NOTE: this method will fail if 
        
        INPUT:
                data_dict        -
                search_strings   - list of substrings 
                action           - one of {'+', '-', '*', '/'}, specifies the operation to compose the new key list
                 
        RETURN: 
                new_data_dict    - 
    """
    def compose_key(self, data_dict, search_strings, new_key, action):
        
        new_data_dict = dict()
        new_list = list()
        
        for key in data_dict.keys():
            for str in search_strings:
                
                if re.search(str, key):
                    if len(new_list) == 0:
                        new_list = data_dict[key]
                    else:
                        if action == '+': 
                            
                            """ catch any errors """
                            try:
                                for i in range(len(new_list)):
                                    new_list[i] = new_list[i] + data_dict[key][i]
                            except IndexError as e:
                                logging.error(e.msg)
                                break;
                
                new_data_dict[key] = data_dict[key]
            
        new_data_dict[new_key] = new_list
        
        return new_data_dict
    
    """
        Only include keys from data_dict that are not matched on strings in search_strings.
        
        INPUT:
                data_dict        -
                search_strings   - list of substrings 
                action           - one of {'+', '-', '*', '/'}, specifies the operation to compose the new key list
                 
        RETURN: 
                new_data_dict    - 
                        -          
    """
    def include_keys(self, data_dict, search_strings):
        
        new_data_dict = dict()
        
        for key in data_dict.keys():
            for str in search_strings:
                """ is the key a super-string of any of the strings in search_strings  """
                if re.search(str, key):
                    new_data_dict[key] = data_dict[key]
            
        return new_data_dict
    
    """
        Remove all keys from data_dict that are not matched on strings in search_strings.
        
         INPUT:
                data_dict        -
                search_strings   - list of substrings 
                action           - one of {'+', '-', '*', '/'}, specifies the operation to compose the new key list
                 
        RETURN: 
                new_data_dict    - 
                        -          
    """
    def exclude_keys(self, data_dict, search_strings):
        
        new_data_dict = dict()
        regExp = ''
        
        for str in search_strings:
                regExp = regExp + '(' + str + ')|'
                
        regExp = regExp[:-1]
 
        for key in data_dict.keys():
            if not(re.search(regExp, key)):
                new_data_dict[key] = data_dict[key]
                    
        return new_data_dict

    
    """
        Return a specific query name given a query type
        
        RETURN: 
                 query_name       - name of the sql file
                
    """
    def get_sql_filename_for_query(self):
        
        try:
            return self._query_names_[self._query_type_]
        except KeyError:
            logging.error('Could not find a query for type: ' + self._query_type_)  
            sys.exit(2)

    """
        Executes a SQL statement and return the raw results.  This is good for generic queries.
        
    """
    def execute_SQL(self, SQL_statement):
        
        try:
            self._cur_.execute(SQL_statement)
            return self._cur_.fetchall()
        
        except Exception as inst:
            
            self._db_.rollback()
            
            # logging.error('Could not execute: ' + SQL_statement)
            logging.error(str(type(inst)))      # the exception instance
            logging.error(str(inst.args))       # arguments stored in .args
            logging.error(inst.__str__())       # __str__ allows args to printed directly

            
            return -1
    
    """
        Takes raw results from a cursor object and sorts them based on a tuple unsigned integer key value
    """
    def sort_results(self, results, key):
        
        return sorted(results, key=operator.itemgetter(key), reverse=False)
    
    """
        Takes a list of tuple results and breaks each tuple key into its own list.  Such lists may be consumed by the matplotlib plotter
    """
    def break_results_into_lists(self, results):
        
        try:
            
            num_properties = len(results[0])
            prop_range_list = range(num_properties)
            lists = list()
            
            for j in prop_range_list:
                lists.append(list())
            
            for i in range(len(results)):
                for j in prop_range_list:
                    lists[j].append(results[i][j])
        
        except:
            pass
        
        return lists
"""

    This Loader inherits the functionality of DaatLoader and handles SQL queries that group data by time intervals.  These are generally preferable for most
    of the time dependent data analysis and also provides functionality that enables the raw results to be combined over all keys
    
"""
class IntervalReportingLoader(DataLoader):
    
    """
        Setup query list
        
    """
    def __init__(self, query_type):
        
        self._query_names_[FDH._QTYPE_BANNER_] = 'report_banner_metrics_minutely'
        self._query_names_[FDH._QTYPE_LP_] = 'report_LP_metrics_minutely'
        self._query_names_[FDH._QTYPE_BANNER_LP_] = 'report_bannerLP_metrics_minutely'
        self._query_names_['campaign'] = 'report_campaign_metrics_minutely'
        self._query_names_['campaign_total'] = 'report_campaign_metrics_minutely_total'
        
        self._query_names_[FDH._QTYPE_BANNER_ + FDH._QTYPE_TIME_] = 'report_banner_metrics_minutely_all'
        self._query_names_[FDH._QTYPE_LP_ + FDH._QTYPE_TIME_] = 'report_lp_metrics_minutely_all'
        self._query_names_[FDH._QTYPE_CAMPAIGN_ + FDH._QTYPE_TIME_] = 'report_campaign_metrics_minutely_all'
        
        self._query_type_ = query_type
    
        """ hardcode the data handler for now """
        self._data_handler_ = FDH
        
        """ Call constructor of parent """
        DataLoader.__init__(self)
        
        self._summary_data_ = None
        
        
    """
        Executes the query which generates interval metrics and sets _results_ and _col_names_
        
        INPUT:
                start_time        - start timestamp for reporting 
                end_time          - end timestamp for reporting
                interval          - minutely interval at which to report metrics
                query_type        - query type: 'banner', 'campaign', 'LP'
                metric_name       - the metric to report
                campaign          - the campaign on which to select
                
            
        RETURN: 
                metrics        - dict containing metric measure for each time index for each donation pipeline handle (e.g. banner names) 
                times          - dict containing time index for each donation pipeline handle (e.g. banner names)
                _results_      - list containing the rows generated by the query
    """
    def run_query(self, start_time, end_time, interval, metric_name, campaign):

        self.init_db()
        
        query_name = self.get_sql_filename_for_query()
        logging.info('Using query: ' + query_name)
        
        metrics = Hlp.AutoVivification()
        times = Hlp.AutoVivification()        
                
        """ Compose datetime objects to represent the first and last intervals """
        start_time_obj = TP.timestamp_to_obj(start_time, 1)
        start_time_obj = start_time_obj.replace(minute=int(math.floor(start_time_obj.minute / interval) * interval))
        start_time_obj_str = TP.timestamp_from_obj(start_time_obj, 1, 3)
        
        end_time_obj = TP.timestamp_to_obj(end_time, 1)
        # end_time_obj = end_time_obj + datetime.timedelta(seconds=-1)
        end_time_obj = end_time_obj.replace(minute=int(math.floor(end_time_obj.minute / interval) * interval))            
        end_time_obj_str = TP.timestamp_from_obj(end_time_obj, 1, 3)
    
        """ QUERY PREP - ONLY EXECUTED IF THE QUERY HAS NOT BEEN RUN ALREADY """
        if not(self._was_run_):
            """ Load the SQL File & Format """
            filename = projSet.__sql_home__+ query_name + '.sql'
            sql_stmnt = Hlp.read_sql(filename)
            
            sql_stmnt = QD.format_query(query_name, sql_stmnt, [start_time, end_time, campaign, interval])
            
        """ Get Indexes into Query """
        key_index = QD.get_key_index(query_name)
        metric_index = QD.get_metric_index(query_name, metric_name)
        time_index = QD.get_time_index(query_name)
        
        """ Compose the data for each separate donor pipeline artifact """
        try:
            """ ONLY EXECUTE THE QUERY IF IT HASN'T BEEN BEFORE """
            if not(self._was_run_):
                logging.info('Running query ...')
                
                self._cur_.execute(sql_stmnt)
                
                """ GET THE COLUMN NAMES FROM THE QUERY RESULTS """
                self._col_names_ = list()
                for i in self._cur_.description:
                    self._col_names_.append(i[0])
                    
                self._results_ = self._cur_.fetchall()
                
                self._was_run_ = True
            
            final_time = dict()                                     # stores the last timestamp seen
            interval_obj = datetime.timedelta(minutes=interval)        # timedelta object used to shift times by _interval_ minutes
            
            for row in self._results_:
                
                key_name = row[key_index]
                time_obj = TP.timestamp_to_obj(row[time_index], 1)  # format = 1, 14-digit TS 
                
                """ For each new dictionary index by key name start a new list if its not already there """    
                try:
                    metrics[key_name].append(row[metric_index])
                    times[key_name].append(time_obj + interval_obj)
                    final_time[key_name] = row[time_index]
                except:
                    metrics[key_name] = list()
                    times[key_name] = list()
                    
                    """ If the first element is not the start time add it 
                        this will be the case if there is no data for the first interval 
                        NOTE:   two datapoints are added at the beginning to define the first interval """
                    times[key_name].append(start_time_obj)
                    times[key_name].append(start_time_obj + interval_obj)
                    
                    if start_time_obj_str != row[time_index]:
                        metrics[key_name].append(0.0)
                        metrics[key_name].append(0.0)
                        
                        times[key_name].append(time_obj)
                        times[key_name].append(time_obj + interval_obj)
                    
                    metrics[key_name].append(row[metric_index])
                    metrics[key_name].append(row[metric_index])
                    
                    final_time[key_name] = row[time_index]
            
            
        except Exception as inst:
            
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)      # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            self._db_.rollback()
            # sys.exit(0)
        

        """ Ensure that the last time in the list is the endtime less the interval """
        
        for key in times.keys():
            if final_time[key] != end_time_obj_str:
                times[key].append(end_time_obj)
                metrics[key].append(0.0)
        
        self.close_db()
        
        """ Convert counts to float (from Decimal) to prevent exception when bar plotting
            Bbox::update_numerix_xy expected numerix array """
        for key in metrics.keys():
            metrics_new = list()
            for i in range(len(metrics[key])):
                
                """ Change null values to 0 """
                if metrics[key][i] == None or metrics[key][i] == 'NULL':
                    metrics[key][i] = 0
                
                metrics_new.append(float(metrics[key][i]))
            metrics[key] = metrics_new
        
        return [metrics, times, self._results_]


    """
        Post process raw data from query.  Combines data rows according to column type definitions.  This must be run *after* run_query.
        
        This allows aggregates of query data to be performed after the actual processing of the query.
    """
    def combine_rows(self):
        
        query_name = self.get_sql_filename_for_query()
        logging.info('Using query: ' + query_name)
        
        col_types = self._data_handler_.get_col_types(self._query_type_)
        key_index = QD.get_key_index(query_name)
        
        data_dict = dict()
        num_rows = len(self._results_)
        
        """ Check that there are columns defined for the query type """
        if len(col_types) == 0:
            logging.info('No metric columns defined for this query type')
            return 0
            
        """ Combine the rows of data according to the column type definition for the given query """
        for row in self._results_:
            
            key = row[key_index]
            
            try:
                data_dict[key] == None  # check for a Index Error
            except KeyError:
                data_dict[key] = dict()
                
            for i in range(len(row)):
                
                col_type = col_types[i]
                field = row[i]
                
                """ Change null values to 0 """
                if field == None or field == 'NULL':
                    field = 0.0
                
                """ 
                    COMBINE THE DATA FOR EACH KEY
                    
                    Based on the column type compile an aggregate (e.g. sum, average) 
                    
                """
                
                if col_type == self._data_handler_._COLTYPE_RATE_:
                    
                    try:
                        data_dict[key][self._col_names_[i]] = data_dict[key][self._col_names_[i]] + float(field)
                    except KeyError:
                        data_dict[key][self._col_names_[i]] = float(field)
                        
                elif col_type == self._data_handler_._COLTYPE_AMOUNT_:
                    
                    try:
                        data_dict[key][self._col_names_[i]] = data_dict[key][self._col_names_[i]] + float(field)
                    except KeyError:
                        data_dict[key][self._col_names_[i]] = float(field)
        
        """ If the dataset is empty flag an error and set the summary data to empty """
        try :
            num_rows = len(self._results_) / len(data_dict.keys())
            
        except ZeroDivisionError:
            logging.error('Empty dataset.  Either no keys were found or the the queried data was empty')
            self._summary_data_ = dict()
            
            return
            
        """ 
            POST PROCESSING
            
            Normalize rate columns 
        """
        for i in range(len(col_types)):
            if col_types[i] == self._data_handler_._COLTYPE_RATE_:
                for key in data_dict.keys():
                    data_dict[key][self._col_names_[i]] = data_dict[key][self._col_names_[i]] / num_rows
        
        self._summary_data_ = data_dict
    


"""

    This class inherits the IntrvalLoader functionality but utilizes the campaign DataLoader instead.  Also the results generated incorporate campaign totals also --
    the result fully specifies each campaign item (campaign - banner - landing page) and the number of views resulting.
    
    This differs from the interval reporter in that it compiles results for views and donations only for an entire donation process or pipeline.

"""
class CampaignIntervalReportingLoader(DataLoader):

    
    def __init__(self):
        self._query_type_ = 'campaign'
        
        self._irl_artifacts_ = IntervalReportingLoader('campaign')
        self._irl_totals_ = IntervalReportingLoader('campaign_total')
        
        
    """
        <DESCRIPTION>
        
        INPUT:
                start_time        - start timestamp for reporting 
                end_time          - end timestamp for reporting
                interval          - minutely interval at which to report metrics
                query_type        - query type: 'banner', 'campaign', 'LP'
                metric_name       - the metric to report
                campaign          - the campaign on which to select
                
            
        RETURN: 
                metrics        - dict containing metric measure for each time index for each donation pipeline handle (e.g. banner names) 
                times          - dict containing time index for each donation pipeline handle (e.g. banner names)
    """
    def run_query(self, start_time, end_time, interval, metric_name, campaign):
        
        """ Execute the standard interval reporting query """
        data = self._irl_artifacts_.run_query(start_time, end_time, interval, metric_name, campaign)
        metrics = data[0] 
        times = data[1]
                
        """ Get the totals for campaign views and donations """
        data = self._irl_totals_.run_query(start_time, end_time, interval, metric_name, campaign)
        metrics_total = data[0] 
        times_total = data[1]

        """ Combine the results for the campaign totals with (banner, landing page, campaign) """
        for key in metrics_total.keys():
            metrics[key] = metrics_total[key]
            times[key] = times_total[key]
                
        return [metrics, times]

    
"""
    This class is concerned with preparing the data for a hypothesis test and is consumed by classes which perform this analysis in HypothesisTest.py
"""
class HypothesisTestLoader(DataLoader):
    
    """
        Constructor
        
        _results_ will be a list storing the data for each artifact
    """
    def __init__(self):
        
        self._results_ = list()
        self._results_.append(list())
        self._results_.append(list())
        
        """ Call constructor of parent """
        DataLoader.__init__(self)
        
        
    """
        Execute data acquisition for hypothesis tester.  The idea is that data sampled identically over time for item 1 and item 2 will be generated on which analysis may be carried out.
        
        !! MODIFY/ FIXME -- the sampling is awkward, sampling interval and test interval should be specified explicitly !!
        
        INPUT:
            query_name     -   non-formatted sql filename  !!MODIFY / FIXME  -- this should be a type instead !!
            metric_name    -   metric to be extracted from the data  
            campaign       - 
            item_1         -   artifact or key name 
            item_2         -   artifact or key name
            start_time     -   
            end_time       - 
            interval       -   test interval; this groups a set of samples together to perform a paired t-test
            num_samples    -   samples per test interval (sampling interval = interval / num_samples)
            
        RETURN:
            metrics_1        - the metrics for item 1
            metrics_2        - the metrics for item 2
            times_indices    - the sampling intervals
       
    """
    def run_query(self, query_name, metric_name, campaign, item_1, item_2, start_time, end_time, interval, num_samples):
        
        """ Verify that the query name is valid """
        if not(query_name == 'report_banner_confidence' or query_name == 'report_LP_confidence' or query_name == 'report_bannerLP_confidence'):
            logging.error('Invalid query name for confidence data sourcing.\n\nUse on of:\n\treport_banner_confidence\n\treport_LP_confidence\n\treport_bannerLP_confidence')
            return [[],[],[]]
        
        logging.info('Using query: ' + query_name)
        
        """ 
            Retrieve time lists with timestamp format 1 (yyyyMMddhhmmss) 
            This breaks the metrics into evenly sampled intervals
        """
        ret = TP.get_time_lists(start_time, end_time, interval, num_samples, 1)
        times = ret[0]
        times_indices = ret[1]
        
        """ ONLY EXECUTE THE QUERY IF IT HASN'T BEEN BEFORE """
        if not(self._was_run_):
            
            self.init_db()
            
            filename = projSet.__sql_home__ + query_name + '.sql'
            sql_stmnt = Hlp.read_sql(filename)
            
        metric_index = QD.get_metric_index(query_name, metric_name)
        metrics_1 = []
        metrics_2 = []
        
        """
            EXECUTE THE QUERIES FOR EACH INTERVAL
            
            Generates metrics for each artifact, sampled in the same way
            
        """
        for i in range(len(times) - 1):
            
            # print '\nExecuting number ' + str(i) + ' batch of of data.'
            t1 = times[i]
            t2 = times[i+1]
            
            if not(self._was_run_):
                formatted_sql_stmnt_1 = QD.format_query(query_name, sql_stmnt, [t1, t2, item_1, campaign])
                formatted_sql_stmnt_2 = QD.format_query(query_name, sql_stmnt, [t1, t2, item_2, campaign])
                
            try:
                
                """ ONLY EXECUTE THE QUERY IF IT HASN'T BEEN BEFORE """
                if not(self._was_run_):
                
                    logging.info('Running confidence queries ...')
                    #err_msg = formatted_sql_stmnt_1
                    
                    self._cur_.execute(formatted_sql_stmnt_1)
                    results_1 = self._cur_.fetchone()  # there should only be a single row
                    self._results_[0].append(results_1)
                    
                    #err_msg = formatted_sql_stmnt_2
                    
                    self._cur_.execute(formatted_sql_stmnt_2)
                    results_2 = self._cur_.fetchone()  # there should only be a single row
                    self._results_[1].append(results_2)
                
            except Exception as inst:
                
                logging.error(type(inst))     # the exception instance
                logging.error(inst.args)     # arguments stored in .args
                logging.error(inst)           # __str__ allows args to printed directly
                    
                self._db_.rollback()
                #sys.exit("Database Interface Exception:\n" + err_msg)
            
            """ If no results are returned in this set the sample value is 0.0 
                !! MODIFY -- these results should not count as data points !! """    
            try:
                metrics_1.append(self._results_[0][i][metric_index])
            except TypeError:
                metrics_1.append(0.0)
            try:
                metrics_2.append(self._results_[1][i][metric_index])
            except TypeError:
                metrics_2.append(0.0)
        
        #print self._results_
        #print metrics_1
        #print metrics_2

        """ ONLY EXECUTE THE QUERY IF IT HASN'T BEEN BEFORE """
        if not(self._was_run_):
            
            self.close_db()
            self._was_run_ = True
        
        # return the metric values at each time
        return [metrics_1, metrics_2, times_indices]
    

"""

    CLASS :: CampaignReportingLoader
    
    This inherits from the DataLoader base class and handles reporting on utm_campaigns.  This reporter handles returning lists of banners and lps from a given campaign and also handles 
    reporting donation totals across all campaigns.  While this interface is concerned with campaign reporting it does not associated results with time intervals within the start and 
    end times 
      
"""
class CampaignReportingLoader(DataLoader):
    
    def __init__(self, query_type):
        self._query_names_['totals'] = 'report_campaign_totals'
        self._query_names_['times'] = 'report_campaign_times'
        self._query_names_[FDH._TESTTYPE_BANNER_] = 'report_campaign_banners'
        self._query_names_[FDH._TESTTYPE_LP_] = 'report_campaign_lps'
        
        self._query_type_ = query_type
        
        """ Call constructor of parent """
        DataLoader.__init__(self)
        
    """
        !! MODIFY / FIXME -- use python reflection !! ... maybe
        
        This method is retrieving campaign names 
       
        delegates the procesing to different methods
        
    """
    def run_query(self, params):
        
        self.init_db()
        
        data = None
        
        if self._query_type_ == 'totals':
            data = self.query_totals(params)
        elif self._query_type_ == FDH._TESTTYPE_BANNER_ or self._query_type_ == FDH._TESTTYPE_LP_:
            data = self.query_artifacts(params)
            
        self.close_db()
        
        return data
    
    """
    
        Handle queries from  "report_campaign_totals"
        
        Gets metric totals across campaigns
        
    """
    def query_totals(self, params):
        
        """ Resolve parameters """
        metric_name = params['metric_name']
        start_time = params['start_time']
        end_time = params['end_time']
        
        query_name = self.get_sql_filename_for_query()
        logging.info('Using query: ' + query_name)
        
        """ Load the SQL File & Format """
        filename = projSet.__sql_home__+ query_name + '.sql'
        sql_stmnt = Hlp.read_sql(filename)
        sql_stmnt = QD.format_query(query_name, sql_stmnt, [start_time, end_time])
        
        """ Get Indexes into Query """
        key_index = QD.get_key_index(query_name)
        metric_index = QD.get_metric_index(query_name, metric_name)
        
        data = Hlp.AutoVivification()
        raw_data = Hlp.AutoVivification()
        
        """ Compose the data for each separate donor pipeline artifact """
        try:
            
            self._cur_.execute(sql_stmnt)
            
            results = self._cur_.fetchall()
            
            for row in results:
                
                key_name = row[key_index]
                data[key_name] = float(row[metric_index])
                raw_data[key_name] = row
         
        except Exception as inst:
            
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)    # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            self._db_.rollback()
            sys.exit(0)


        return data, raw_data
    
    """
    
        Handle queries from "report_campaign_banners" and "report_campaign_lps" 
        
        Gets a list of banners and landing pages running for a given campaign over a given time frame
        
    """
    def query_artifacts(self, params):
        
        """ Resolve parameters """
        utm_campaign = params['utm_campaign']
        start_time = params['start_time']
        end_time = params['end_time']
        
        query_name = self.get_sql_filename_for_query()
        logging.info('Using query: ' + query_name)
        
        """ Load the SQL File & Format """
        filename = projSet.__sql_home__+ query_name + '.sql'
        sql_stmnt = Hlp.read_sql(filename)        
        sql_stmnt = QD.format_query(query_name, sql_stmnt, [start_time, end_time, utm_campaign])
        
        """ Get Indexes into Query """
        key_index = QD.get_key_index(query_name)    
        
        data = list()
        
        """ Compose the data for each separate donor pipeline artifact """
        try:
            
            self._cur_.execute(sql_stmnt)
            
            results = self._cur_.fetchall()
            
            for row in results:
                data.append(row[key_index])
                # key_name = row[key_index]
                
        except Exception as inst:
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)    # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            self._db_.rollback()
            sys.exit(0)


        return data


"""
    This DataLoader class handles queries reporting on the donation amount profiles of (a) campaign(s):
    
        report
"""
class DonorBracketsReportingLoader(DataLoader):
    
    """
        constructor
        
        @param query_type:  indicates the type of artifact reported by the query
        
                FDH._QTYPE_BANNER_
                FDH._QTYPE_LP_
                FDH._QTYPE_CAMPAIGN_
    """
    def __init__(self, query_type):
        
        self.init_db()
        
        """ Use _query_names_ to store a single query name """
        self._query_names_ = 'report_donor_dollar_breakdown' # embed the query name in the class itself
        self._query_type_ = query_type
        
    """ Close the connection """
    def __del__(self):
        self.close_db()
    
    
    """
        Retrieve the counts for donation brackets and process to pass for reporting
    """
    def run_query(self, start_time, end_time, campaign):
        
        query_name = self._query_names_
        logging.info('Using query: ' + query_name)
        
        """ Load the SQL File & Format """
        filename = projSet.__sql_home__ + query_name + '.sql'
        sql_stmnt = Hlp.read_sql(filename)

        """ Place comments in the query string to"""
        if self._query_type_ == FDH._QTYPE_CAMPAIGN_:
            sql_stmnt = sql_stmnt % ("utm_campaign as artifact,", start_time, end_time, campaign)
        elif self._query_type_ == FDH._QTYPE_BANNER_:
            sql_stmnt = sql_stmnt % ("SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as artifact,", start_time, end_time, campaign)
        elif self._query_type_ == FDH._QTYPE_LP_:
            sql_stmnt = sql_stmnt % ("SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-11) as artifact,", start_time, end_time, campaign)
        
        logging.info('Query Complete: ' + query_name)
        
        """ Execute the query """
        try:
            self._cur_.execute(sql_stmnt)
            self._results_ = self._cur_.fetchall()
        
        except Exception as inst:
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)    # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            self._db_.rollback()
            sys.exit(0)
        
        
        """ Dictionaries to store the results - results are keyed by the artifact name (banner, LP, campaign) """
        
        bracket_names = dict()
        donations = dict()
        amounts = dict()
        # bracket_values = dict()
        
        """ Process the results - break into separate dictionaries """
        
        for row in self._results_:
            
            artifact = self.get_query_record_field(row, 'artifact')
            
            if artifact not in bracket_names.keys():
                
                bracket_names[artifact] = list()
                donations[artifact] = list()
                amounts[artifact] = list()
            
            """ The name of the donor bracket - the first four characters may be omitted """
            bracket_name = self.get_query_record_field(row, 'bracket_name')[4:]
            
            bracket_names[artifact].append( bracket_name )
            donations[artifact].append(  int(self.get_query_record_field(row, 'count')) )
            amounts[artifact].append(  self.get_query_record_field(row, 'total_amount') )
            
            """ For the largest bracket just use its min_val as the index 
            if re.search('>', bracket_name):
                bracket_values[artifact].append( float(self.get_query_record_field(row, 'min_val') ) )
            else:
                bracket_values[artifact].append( ( float(self.get_query_record_field(row, 'max_val')) + float(self.get_query_record_field(row, 'min_val')) )  / 2 )
            """
        # return bracket_names, donations, amounts
        return self._add_missing_brackets(bracket_names, donations, amounts)
    
    """ 
        Add missing brackets -- helper to run_query 
    """
    def _add_missing_brackets(self, bracket_names, donations, amounts):
    
        brackets = DonorBracketsTableLoader().get_all_brackets()
        
        """ Truncate brackets to match what has been extracted from the db """
        indices = range(len(brackets))
        for i in indices:
            brackets[i] = brackets[i][4:]
            
        queried_brackets = bracket_names.keys()
        
        """ new data dictionaries """
        new_bracket_names = dict()
        new_donations = dict()
        new_amounts = dict()
        
        """ add keys to new lists """
        for key in queried_brackets:
            new_bracket_names[key] = list()
            new_donations[key] = list()
            new_amounts[key] = list()
        
        """  """
        index = dict()
        for key in queried_brackets:
            index[key] = 0
            
        for key in queried_brackets:
            for i in brackets:
                if i not in bracket_names[key]:
                    new_bracket_names[key].append(i)
                    new_donations[key].append(0.01)
                    new_amounts[key].append(0.01)
                else:
                    new_bracket_names[key].append(bracket_names[key][index[key]])
                    new_donations[key].append(donations[key][index[key]])
                    new_amounts[key].append(amounts[key][index[key]])
                    index[key] = index[key] + 1

        return new_bracket_names, new_donations, new_amounts
    
    
    """
        Turn a counts vector into a set of samples || TODO:  bump this up to the base class
    """
    def histify(self, data, label_indices):
        
        indices = range(len(data))
        hist_list = list()

        for index in indices:
            samples = [label_indices[index]] * data[index]            
            hist_list.extend(samples)
            
        return hist_list
    
    """
        Retrieve 
        
        Fields: artifact (varchar) | bracket_name (varchar) | count (int) | total_amount (decimal)
    """
    def get_query_record_field(self, row, key):
        
        try:
            if key == 'artifact':
                return row[0]
            if key == 'bracket_name':
                return row[1]
            elif key == 'count':
                return row[2]
            elif key == 'total_amount':           
                return row[3]
            elif key == 'min_val':           
                return row[4]
            elif key == 'max_val':           
                return row[5]
        
        except Exception as inst:
            
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)      # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            return ''
        
           
"""

    CLASS :: TableLoader
    
    Base class for providing MySQL table access.  Inherits DataLoader.
    
    METHODS:
            record_exists     - return a boolean value reflecting whether a record exists in the table
            insert_row        - try to insert a new record into the table
            delete_row        - try to delete a record from the table
            update            - try to modify a record in the table
            
"""
class TableLoader(DataLoader):
    
    def record_exists(self, **kwargs):
        return
    
    def insert_row(self, **kwargs):
        return
    
    def delete_row(self, **kwargs):
        return
    
    def update_row(self, **kwargs):
        return
    
"""

    CLASS :: TTestLoaderHelp
    
    Provides data access particular to the t-test
    
    storage3.pmtpa.wmnet.faulkner.ttest:
        
    +--------------------+--------------+------+-----+---------+-------+
    | Field              | Type         | Null | Key | Default | Extra |
    +--------------------+--------------+------+-----+---------+-------+
    | degrees_of_freedom | int(2)       | YES  |     | NULL    |       | 
    | p                  | decimal(7,6) | YES  |     | NULL    |       | 
    | t                  | decimal(5,4) | YES  |     | NULL    |       | 
    +--------------------+--------------+------+-----+---------+-------+


"""
class TTestLoaderHelp(TableLoader):
    
    def __init__(self):
        self.init_db()
    
    def __del__(self):
        self.close_db()
 
    """
    This method knows about faulkner.t_test.  This is a lookup table for p-values
    given the degrees of freedom and statistic t test
    
    INPUT:
        degrees_of_freedom     - computed degrees of freedom of a dataset modeled on a student's t distribution
        t                      - test statistic; random variable whose value is to be measured

    RETURN:
        p        - the highest p value based on the input 
   
    """
    def get_pValue(self, degrees_of_freedom, t):
        
        select_stmnt = 'select max(p) from t_test where degrees_of_freedom = ' + str(degrees_of_freedom) + ' and t >= ' + str(t)

        try:
            self._cur_.execute(select_stmnt)
            results = self._cur_.fetchone()
                
            if results[0] != None:
                p = float(results[0])
            else:
                p = .0005
        except:
            self._db_.rollback()
            sys.exit('Could not execute: ' + select_stmnt)
            
        return p


    
"""

    CLASS :: TestTableLoader
    
    storage3.pmtpa.wmnet.faulkner.test:
        
    +---------------+----------------+------+-----+---------------------+-----------------------------+
    | Field         | Type           | Null | Key | Default             | Extra                       |
    +---------------+----------------+------+-----+---------------------+-----------------------------+
    | test_name     | varchar(50)    | YES  |     |                     |                             | 
    | test_type     | varchar(20)    | YES  |     |                     |                             | 
    | utm_campaign  | varchar(128)   | NO   | PRI | NULL                |                             | 
    | start_time    | timestamp      | NO   |     | CURRENT_TIMESTAMP   | on update CURRENT_TIMESTAMP | 
    | end_time      | timestamp      | NO   |     | 0000-00-00 00:00:00 |                             | 
    | winner        | varchar(128)   | YES  |     | NULL                |                             | 
    | is_conclusive | binary(1)      | YES  |     | NULL                |                             | 
    | html_report   | varchar(10000) | YES  |     | NULL                |                             | 
    +---------------+----------------+------+-----+---------------------+-----------------------------+


            
"""
class TestTableLoader(TableLoader):
    
    def __init__(self):
        self.init_db()
    
    def __del__(self):
        self.close_db()

    def process_kwargs(self, kwargs_dict):
        
        utm_campaign = 'NULL'
        test_type = 'NULL'
        start_time =  'NULL'
        end_time =  'NULL'
        winner = 'NULL'
        is_conclusive = 'NULL'
        html_report = 'NULL'
        test_name = 'NULL'
        
        for key in kwargs_dict:
            if key == 'utm_campaign':           
                utm_campaign = Hlp.stringify(kwargs_dict[key])
            elif key == 'test_name':
                test_name = Hlp.stringify(kwargs_dict[key])
            elif key == 'test_type':
                test_type = Hlp.stringify(kwargs_dict[key])
            elif key == 'start_time':
                start_time = Hlp.stringify(kwargs_dict[key])
            elif key == 'end_time':
                end_time = Hlp.stringify(kwargs_dict[key])
            elif key == 'winner':
                winner = Hlp.stringify(kwargs_dict[key])
            elif key == 'is_conclusive':
                is_conclusive = Hlp.stringify(kwargs_dict[key])
            elif key == 'html_report':                
                html_report = kwargs_dict[key]
        
        return [test_name, test_type, utm_campaign, start_time, end_time, winner, is_conclusive, html_report]
            
            
    def update_test_row(self, **kwargs):
        
        test_name, test_type, utm_campaign, start_time, end_time, winner, is_conclusive, html_report = self.process_kwargs(kwargs)
        
        cols = ' test_name = ' + test_name + ', test_type = ' + test_type + ', utm_campaign = ' +  utm_campaign + ', start_time = ' +  start_time + ', end_time = ' +  end_time + ', winner = ' +  winner + ', is_conclusive = ' +  is_conclusive + ', html_report = ' +  html_report 
        update_stmnt = 'update test set' + cols + ' where utm_campaign = ' + utm_campaign
        
        try:
            return self._cur_.execute(update_stmnt)
        except:
            self._db_.rollback()
            logging.error('Could not execute: ' + update_stmnt)
            
            return -1
        

    
    def insert_row(self, **kwargs):
        
        insert_stmnt = 'insert into test values '
        
        test_name, test_type, utm_campaign, start_time, end_time, winner, is_conclusive, html_report = self.process_kwargs(kwargs)
    
        insert_stmnt = insert_stmnt + '(' + test_name + ',' + test_type + ',' + utm_campaign + ',' + start_time + ',' + end_time + ',' + winner + ',' + is_conclusive + ',' + html_report + ')'
        
        try:
            return self._cur_.execute(insert_stmnt)
        except:
            self._db_.rollback()
            logging.error('Could not execute: ' + insert_stmnt)
            
            return -1
            # sys.exit('Could not execute: ' + insert_stmnt)

    
    def record_exists(self, **kwargs):
        
        test_name, test_type, utm_campaign, start_time, end_time, winner, is_conclusive, html_report = self.process_kwargs(kwargs)
        
        if utm_campaign != 'NULL' and utm_campaign != None: 
            select_stmnt = 'select * from test where utm_campaign = ' + utm_campaign
            
            try:
                self._cur_.execute(select_stmnt)
                results = self._cur_.fetchone()
                
            except:
                results = None
                self._db_.rollback()
                logging.error('Could not execute: ' + select_stmnt)
                # sys.exit('Could not execute: ' + select_stmnt)

            if results is None:
                return False
            else:
                return True
            
        else:
            logging.error("A utm_campaign must be specified (e.g. record_exists(utm_campaign='smthg'))")
            return -1
    
    
    def get_test_row(self, utm_campaign):
        
        select_stmnt = 'select * from test where utm_campaign = ' + Hlp.stringify(utm_campaign)
        
        try:
            self._cur_.execute(select_stmnt)
            results = self._cur_.fetchone()
            
        except:
            results = None
            self._db_.rollback()
            logging.error('Could not execute: ' + select_stmnt)
        
        return results
    
    
    def get_all_test_rows(self):
        
        select_stmnt = 'select * from test'
        
        try:
            self._cur_.execute(select_stmnt)
            results = self._cur_.fetchall()
            
        except:
            results = None
            self._db_.rollback()
            logging.error('Could not execute: ' + select_stmnt)
            # sys.exit('Could not execute: ' + select_stmnt)
        
        return results
        
    """
        This method handles mapping test row fields to col names
    """
    def get_test_field(self, row, key):
        
        try:
            if key == 'test_name':
                return row[0]
            elif key == 'test_type':
                return row[1]
            elif key == 'utm_campaign':           
                return row[2]
            elif key == 'start_time':
                return row[3].__str__()
            elif key == 'end_time':
                return row[4].__str__()
            elif key == 'winner':
                return row[5]
            elif key == 'is_conclusive':
                return row[6]
            elif key == 'html_report':                
                return row[7]
        
        except Exception as inst:
            
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)      # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            return ''

    
    def delete_row(self):
        return
    
    
"""

    CLASS :: SquidLogTableLoader
    
    storage3.pmtpa.wmnet.faulkner.table squid_log_record :
    
    +--------------------+------------------+------+-----+---------------------+-----------------------------+
    | Field              | Type             | Null | Key | Default             | Extra                       |
    +--------------------+------------------+------+-----+---------------------+-----------------------------+
    | type               | varchar(20)      | YES  |     |                     |                             | 
    | log_copy_time      | timestamp        | NO   | PRI | CURRENT_TIMESTAMP   | on update CURRENT_TIMESTAMP | 
    | start_time         | timestamp        | NO   |     | 0000-00-00 00:00:00 |                             | 
    | end_time           | timestamp        | NO   |     | 0000-00-00 00:00:00 |                             | 
    | log_completion_pct | int(3)           | YES  |     | 0                   |                             | 
    | total_rows         | int(10) unsigned | YES  |     | NULL                |                             | 
    +--------------------+------------------+------+-----+---------------------+-----------------------------+

            
"""
class SquidLogTableLoader(TableLoader):
    
    def __init__(self):
        self.init_db()
    
    def __del__(self):
        self.close_db()


    def process_kwargs(self, kwargs_dict):
        
        type = 'NULL'
        log_copy_time = 'NULL'
        start_time =  'NULL'
        end_time =  'NULL'
        log_completion_pct = 'NULL'
        total_rows = 'NULL'

        
        for key in kwargs_dict:
            if key == 'type':           
                type = Hlp.stringify(kwargs_dict[key])
            elif key == 'log_copy_time':
                log_copy_time = Hlp.stringify(kwargs_dict[key])
            elif key == 'start_time':
                start_time = Hlp.stringify(kwargs_dict[key])
            elif key == 'end_time':
                end_time = Hlp.stringify(kwargs_dict[key])
            elif key == 'log_completion_pct':
                log_completion_pct = kwargs_dict[key]
            elif key == 'total_rows':
                total_rows = kwargs_dict[key]
        
        return [type, log_copy_time, start_time, end_time, log_completion_pct, total_rows]
    
    def insert_row(self, **kwargs):
        
        insert_stmnt = 'insert into squid_log_record values '
        
        type, log_copy_time, start_time, end_time, log_completion_pct, total_rows = self.process_kwargs(kwargs)
    
        insert_stmnt = insert_stmnt + '(' + type + ',' + log_copy_time + ',' + start_time + ',' + end_time + ',' + log_completion_pct + ',' + total_rows + ')'
        
        try:
            self._cur_.execute(insert_stmnt)
        except:
            self._db_.rollback()
            logging.error('Could not execute: ' + insert_stmnt)
            
            return -1
            # sys.exit('Could not execute: ' + insert_stmnt)

        return 0

    
    def get_table_row(self, log_copy_time):
        
        select_stmnt = 'select * from squid_log_record where log_copy_time = ' + Hlp.stringify(log_copy_time)
        
        try:
            self._cur_.execute(select_stmnt)
            results = self._cur_.fetchone()
            
        except:
            results = None
            self._db_.rollback()
            logging.error('Could not execute: ' + select_stmnt)
        
        return results
    
    
    def get_all_table_rows(self):
        
        select_stmnt = 'select * from squid_log_record'
        
        try:
            self._cur_.execute(select_stmnt)
            results = self._cur_.fetchall()
            
        except:
            results = None
            self._db_.rollback()
            logging.error('Could not execute: ' + select_stmnt)
            # sys.exit('Could not execute: ' + select_stmnt)
        
        return results
    
    
    def update_table_row(self, **kwargs):
        
        type, log_copy_time, start_time, end_time, log_completion_pct, total_rows = self.process_kwargs(kwargs)
        
        cols = ' type = ' + type + ', log_copy_time = ' + log_copy_time + ', start_time = ' +  start_time + ', end_time = ' +  end_time + ', log_completion_pct = ' +  log_completion_pct + ', total_rows = ' +  total_rows
        update_stmnt = 'update squid_log_record set' + cols + ' where log_copy_time = ' + log_copy_time
        
        try:
            self._cur_.execute(update_stmnt)
        except:
            self._db_.rollback()
            logging.error('Could not execute: ' + update_stmnt)
            
            return -1
        
        return 0
    
    
    def get_all_rows_unique_start_time(self):
        
        select_stmnt = 'select type, log_copy_time, start_time, end_time, log_completion_pct, total_rows from ' \
        '(select type as temp_type, max(log_copy_time) as max_copy_time from squid_log_record group by type, start_time) as temp join ' \
        'squid_log_record on (max_copy_time = squid_log_record.log_copy_time and temp_type = type)'
        
        try:
            self._cur_.execute(select_stmnt)
            results = self._cur_.fetchall()
        except:
            self._db_.rollback()
            logging.error('Could not execute: ' + select_stmnt)
            return -1
            
        return results
    
    def get_completion_rate_of_latest_log(self):
        
        select_stmnt = 'select type, start_time, log_completion_pct, log_completion_pct from (select max(log_copy_time) as max_copy_time from squid_log_record) as temp join squid_log_record on max_copy_time = squid_log_record.log_copy_time'
        
        try:
            self._cur_.execute(select_stmnt)
            results = self._cur_.fetchall()
        except:
            self._db_.rollback()
            logging.error('Could not execute: ' + select_stmnt)
            return -1
        
        return results
    
    """
        This method handles mapping test row fields to col names
        
    """
    def get_squid_log_record_field(self, row, key):
        
        try:
            if key == 'type':
                return row[0]
            elif key == 'log_copy_time':
                return row[1].__str__()
            elif key == 'start_time':           
                return row[2].__str__()
            elif key == 'end_time':
                return row[3].__str__()
            elif key == 'log_completion_pct':
                return row[4]
            elif key == 'total_rows':
                return row[5]
        
        except Exception as inst:
            
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)      # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            return ''
    
"""

    CLASS :: ImpressionTableLoader
    
    storage3.pmtpa.wmnet.faulkner.banner_impressions :
        
    +-----------------+------------------+------+-----+---------------------+-----------------------------+
    | Field           | Type             | Null | Key | Default             | Extra                       |
    +-----------------+------------------+------+-----+---------------------+-----------------------------+
    | start_timestamp | timestamp        | NO   |     | CURRENT_TIMESTAMP   | on update CURRENT_TIMESTAMP | 
    | utm_source      | varchar(128)     | YES  |     | NULL                |                             | 
    | referrer        | varchar(128)     | YES  |     | NULL                |                             | 
    | country         | varchar(128)     | YES  |     | NULL                |                             | 
    | lang            | varchar(20)      | YES  |     | NULL                |                             | 
    | counts          | int(10) unsigned | YES  |     | NULL                |                             | 
    | on_minute       | timestamp        | NO   |     | 0000-00-00 00:00:00 |                             | 
    +-----------------+------------------+------+-----+---------------------+-----------------------------+
            
"""
class ImpressionTableLoader(TableLoader):
    
    def __init__(self):
        self.init_db()
    
    def __del__(self):
        self.close_db()


    def process_kwargs(self, kwargs_dict):
        
        start_timestamp = 'NULL'
        utm_source = 'NULL'
        referrer = 'NULL'
        country =  'NULL'
        lang =  'NULL'
        counts = 'NULL'
        on_minute = 'NULL'
                
        for key in kwargs_dict:
            if key == 'utm_source_arg':           
                utm_source = Hlp.stringify(kwargs_dict[key])
            elif key == 'referrer_arg':
                referrer = Hlp.stringify(kwargs_dict[key])
            elif key == 'country_arg':
                country = Hlp.stringify(kwargs_dict[key])
            elif key == 'lang_arg':
                lang = Hlp.stringify(kwargs_dict[key])
            elif key == 'counts_arg':
                counts = Hlp.stringify(kwargs_dict[key])
            elif key == 'on_minute_arg':
                on_minute = kwargs_dict[key]
            elif key == 'start_timestamp_arg':
                start_timestamp = kwargs_dict[key]
        
        return [start_timestamp, utm_source, referrer, country, lang, counts, on_minute]
    
    
    def insert_row(self, **kwargs):
        
        insert_stmnt = 'insert into banner_impressions values '
        
        start_timestamp, utm_source, referrer, country, lang, counts, on_minute = self.process_kwargs(kwargs)
    
        val = '(' + start_timestamp + ',' + utm_source + ',' + referrer + ',' + country + ',' + lang + ',' \
                                    + counts + ',' + on_minute + ');'
                                    
        insert_stmnt = insert_stmnt + val

        try:
            self._cur_.execute(insert_stmnt)
        except:
            self._db_.rollback()
            logging.error('Could not execute: ' + insert_stmnt)
            
            return -1
            # sys.exit('Could not execute: ' + insert_stmnt)

        return 0
    
    
    def delete_row(self, start_timestamp):
        
        deleteStmnt = 'delete from banner_impressions where start_timestamp = \'' + start_timestamp + '\';'
        
        self.execute_SQL(deleteStmnt)
        
    
"""

    CLASS :: LandingPageTableLoader
    
    storage3.pmtpa.wmnet.faulkner.landing_page :
    
    +--------------+---------------+------+-----+-------------------+-----------------------------+
    | Field        | Type          | Null | Key | Default           | Extra                       |
    +--------------+---------------+------+-----+-------------------+-----------------------------+
    | utm_source   | varchar(128)  | YES  | MUL | NULL              |                             | 
    | utm_campaign | varchar(128)  | YES  | MUL | NULL              |                             | 
    | utm_medium   | varchar(128)  | YES  |     | NULL              |                             | 
    | landing_page | varchar(128)  | YES  | MUL | NULL              |                             | 
    | page_url     | varchar(1000) | YES  |     | NULL              |                             | 
    | referrer_url | varchar(1000) | YES  |     | NULL              |                             | 
    | browser      | varchar(50)   | YES  |     | NULL              |                             | 
    | lang         | varchar(20)   | YES  |     | NULL              |                             | 
    | country      | varchar(20)   | YES  | MUL | NULL              |                             | 
    | project      | varchar(128)  | YES  |     | NULL              |                             | 
    | ip           | varchar(20)   | YES  |     | NULL              |                             | 
    | request_time | timestamp     | NO   | MUL | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP | 
    | id           | int(11)       | NO   | PRI | NULL              | auto_increment              | 
    +--------------+---------------+------+-----+-------------------+-----------------------------+

            
"""
class LandingPageTableLoader(TableLoader):
    
    def __init__(self):
        self.init_db()
    
    def __del__(self):
        self.close_db()
        
    def process_kwargs(self, kwargs_dict):
        
        utm_source = 'NULL'
        utm_campaign = 'NULL'
        utm_medium = 'NULL'
        landing_page = 'NULL'
        page_url =  'NULL'
        referrer_url =  'NULL'
        browser = 'NULL'
        lang = 'NULL'
        country = 'NULL'
        project = 'NULL'
        ip = 'NULL'
        
        
        for key in kwargs_dict:
            if key == 'utm_source_arg':           
                utm_source = Hlp.stringify(kwargs_dict[key])
            elif key == 'utm_campaign_arg':
                utm_campaign = Hlp.stringify(kwargs_dict[key])
            elif key == 'utm_medium_arg':
                utm_medium = Hlp.stringify(kwargs_dict[key])
            elif key == 'landing_page_arg':
                landing_page = Hlp.stringify(kwargs_dict[key])
            elif key == 'page_url_arg':
                page_url = Hlp.stringify(kwargs_dict[key])
            elif key == 'referrer_url_arg':
                referrer_url = Hlp.stringify(kwargs_dict[key])
            elif key == 'browser_arg':
                browser = Hlp.stringify(kwargs_dict[key])
            elif key == 'lang_arg':
                lang = Hlp.stringify(kwargs_dict[key])
            elif key == 'country_arg':
                country = Hlp.stringify(kwargs_dict[key])
            elif key == 'project_arg':
                project = Hlp.stringify(kwargs_dict[key])
            elif key == 'ip_arg':
                ip = Hlp.stringify(kwargs_dict[key])
            elif key == 'start_timestamp_arg':
                start_timestamp = Hlp.stringify(kwargs_dict[key])
            elif key == 'timestamp_arg':
                timestamp = Hlp.stringify(kwargs_dict[key])
                
        return [start_timestamp, timestamp, utm_source, utm_campaign, utm_medium, landing_page, page_url, referrer_url, browser, lang, country, project, ip]
    
    
    def insert_row(self, **kwargs):
        
        insert_stmnt = 'insert into landing_page_requests values '
        
        start_timestamp, timestamp, utm_source, utm_campaign, utm_medium, landing_page, page_url, referrer_url, browser, lang, country, project, ip = self.process_kwargs(kwargs)
        
        val = '(' + 'convert(' + start_timestamp + ', datetime)' + ',' + utm_source + ',' + utm_campaign + ',' + utm_medium + ',' + landing_page + \
                    ',' + page_url + ',' + referrer_url + ',' + browser + ',' + lang + ',' + country + ','  \
                    + project + ',' +  ip + ',' + 'convert(' + timestamp + ', datetime)' + ');'
                    
        insert_stmnt = insert_stmnt + val
        
        return self.execute_SQL(insert_stmnt)
        
    
    
    def delete_row(self, start_timestamp):
        
        deleteStmnt = 'delete from landing_page_requests where start_timestamp = \'' + start_timestamp + '\';'
        
        return self.execute_SQL(deleteStmnt)

    """
        Retrieve the page ids for referrers from a given log start timestamp 
    """
    def get_referrers(self, start_timestamp):
        
        select_stmnt = "select referrer_url from landing_page_requests where start_timestamp = '%s';" % start_timestamp
        select_stmt_page_ids = 'select page_id from page where%sand page_namespace = 0;'
        
        referrer_urls = list()
        referrers = list()
        ref_ids = list()
        
        results = self.execute_SQL(select_stmnt)
        
        for row in results:
            referrer_urls.append(row[0])
        
        """ Determine if the urls originate at some article - Get the referrers title """
        page_titles = ' ('
        for ref_url in referrer_urls:
            if (re.search('wikipedia.org/wiki/', ref_url)):
                ref_title = ref_url.split('wikipedia.org/wiki/')[1]
                referrers.append(ref_title)
                page_titles = page_titles + 'page_title = \'%s\' or ' % ref_title
        
        if len(referrers) > 0:
            page_titles = page_titles[:-4] + ') '
        else:
            return []
                
        """ Connect to 'enwiki' """    
        self.establish_enwiki_conn()
        
        """ Get the page ids for the referrers """
        results = self.execute_SQL(select_stmt_page_ids % page_titles)
        for row in results:
            ref_ids.append(int(row[0]))
        
        """ Restore connection to 'faulkner' """
        self.establish_faulkner_conn()
    
        return ref_ids
    
    """
        Returns the timestamp start keys appearing within a timeframe
        
        @param start_time:  timestamp string of the start time
        @param end_time:  timestamp string of the end time     
    """
    def get_log_start_times(self, start_time, end_time):
        
        timestamps = list()
        sql = "select distinct(start_timestamp) from landing_page_requests where request_time >= '%s' and request_time < '%s'" % (start_time, end_time)
        results = self.execute_SQL(sql)
        
        for row in results:
            timestamps.append(row[0])
        
        return timestamps
        
"""

    CLASS :: IPCountryTableLoader
    
    storage3.pmtpa.wmnet.faulkner.ip_country :
    
    +---------------+-------------+------+-----+---------+-------+
    | Field         | Type        | Null | Key | Default | Extra |
    +---------------+-------------+------+-----+---------+-------+
    | ip_from       | varchar(50) | NO   | PRI |         |       | 
    | ip_to         | varchar(50) | YES  |     | NULL    |       | 
    | registry      | varchar(50) | YES  |     | NULL    |       | 
    | assigned      | varchar(50) | YES  |     | NULL    |       | 
    | country_ISO_1 | varchar(50) | YES  |     | NULL    |       | 
    | country_ISO_2 | varchar(50) | YES  |     | NULL    |       | 
    | country_name  | varchar(50) | YES  |     | NULL    |       | 
    +---------------+-------------+------+-----+---------+-------+
"""
class IPCountryTableLoader(TableLoader):
    
    def __init__(self):
        self.init_db()
    
    def __del__(self):
        self.close_db()


    """ Given an IP localizes the country """
    def localize_IP(self, ip_string):
        
        # compute ip number
        ip_fields = ip_string.split('.')
        w = int(ip_fields[0])
        x = int(ip_fields[1])
        y = int(ip_fields[2])
        z = int(ip_fields[3])
        
        ip_num = 16777216 * w + 65536 * x + 256 * y + z;
        
        sql_stmnt = 'select country_ISO_1 from ip_country where ' + str(ip_num) + ' >= ip_from and ' + str(ip_num) + ' <= ip_to'
        
        try:
            self._cur_.execute(sql_stmnt)
            row = self._cur_.fetchone()
        
        except:    
            self._db_.rollback()
            logging.error('Could not execute: ' + sql_stmnt)
        
        try:
            country = row[0]
        except:
            country = ''
        
        return country
    
    
    """ Load  data into the IP localization table to associate IPs with countries """
    def load_IP_localization_table(self):
        
        """ Get db object / Create cursor  """
        
        # Parse CSV file 
        ipReader = csv.reader(open('./csv/IpToCountry.csv', 'rb'))
        insert_stmnt = 'INSERT INTO ip_country VALUES '
        # (ip_from,ip_to,registry,assigned,country_ISO_1,country_ISO_2,country_name) 
        header = 1
        for row in ipReader:
            # skip the csv comments
            if row[0][0] != '#':
                header = 0
            
            if not(header):
                
                for i in range(len(row)):
                    pieces = row[i].split('\'')
                    
                    if len(pieces) > 1:
                        new_str = pieces[0]
                        
                        # remove single quotes from fields
                        for j in range(1,len(pieces)):
                            new_str = new_str + ' ' + pieces[j]
                    
                        row[i] = new_str
                            
                vals = '\',\''.join(row)
                sql_stmnt = insert_stmnt + '(\'' + vals + '\')'
                
                #cur.execute(sql_stmnt)
                try:
                    self._cur_.execute(sql_stmnt)
                except:
                    self._db_.rollback()
                    logging.error('Could not insert: ' + sql_stmnt)        


"""

    CLASS :: MiningPatternsTableLoader
    
    storage3.pmtpa.wmnet.faulkner.mining_patterns :
    
    +--------------+-------------+------+-----+---------+-------+
    | Field        | Type        | Null | Key | Default | Extra |
    +--------------+-------------+------+-----+---------+-------+
    | pattern_type | varchar(50) | NO   | PRI |         |       |
    | pattern      | varchar(50) | NO   | PRI |         |       |
    +--------------+-------------+------+-----+---------+-------+

"""
class MiningPatternsTableLoader(TableLoader):
    
    def __init__(self):
        self.init_db()
    
    def __del__(self):
        self.close_db()
        
    def process_kwargs(self, kwargs_dict):
        
        pattern_type = 'NULL'
        pattern = 'NULL'        
        
        for key in kwargs_dict:
            if key == 'pattern_type':           
                pattern_type = Hlp.stringify(kwargs_dict[key])
            elif key == 'pattern':
                pattern = Hlp.stringify(kwargs_dict[key])

                
        return [pattern_type, pattern]
    
    
    def get_all_rows(self):
        
        select_stmnt = 'select * from mining_patterns'
        try:
            self._cur_.execute(select_stmnt)
            results = self._cur_.fetchall()
        except:
            self._db_.rollback()
            logging.error('Could not execute: ' + select_stmnt)
            
        return results
    
    
    def insert_row(self, **kwargs):
        
        insert_stmnt = 'insert into mining_patterns values '
        
        pattern_type, pattern = self.process_kwargs(kwargs)
        
        val = '(' + pattern_type + ',' + pattern + ');'
                    
        insert_stmnt = insert_stmnt + val
        
        return self.execute_SQL(insert_stmnt)
        
    
    
    def delete_row(self, pattern_type, pattern):
        
        deleteStmnt = 'delete from mining_patterns where pattern_type = \'%s\' and pattern = \'%s\';' % (pattern_type, pattern)
        
        return self.execute_SQL(deleteStmnt)


    """
        This method handles mapping test row fields to col names
        
    """
    def get_mining_pattern_field(self, row, key):
        
        try:
            if key == 'pattern_type':
                return row[0]
            elif key == 'pattern':
                return row[1]
        
        except Exception as inst:
            
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)      # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            return ''
    
    
    """
        return the banner and landing page patterns as lists
    """
    def get_pattern_lists(self):
    
        results = self.get_all_rows()
        
        banner_patterns = list()
        lp_patterns = list()
        
        for row in results:
            if self.get_mining_pattern_field(row, 'pattern_type') == 'banner':
                banner_patterns.append(self.get_mining_pattern_field(row, 'pattern'))
            elif self.get_mining_pattern_field(row, 'pattern_type') == 'lp':
                lp_patterns.append(self.get_mining_pattern_field(row, 'pattern'))
                
        return banner_patterns, lp_patterns
    

"""

    CLASS :: DonorBracketsTableLoader
    
    storage3.pmtpa.wmnet.faulkner.donor_brackets :
        
    +--------------+-------------+------+-----+---------+-------+
    | Field        | Type        | Null | Key | Default | Extra |
    +--------------+-------------+------+-----+---------+-------+
    | bracket_name | varchar(20) | YES  |     | NULL    |       |
    | min_val      | int(10)     | YES  |     | NULL    |       |
    | max_val      | int(10)     | YES  |     | NULL    |       |
    +--------------+-------------+------+-----+---------+-------+

"""
class DonorBracketsTableLoader(TableLoader):
    
    def __init__(self):
        self.init_db()
    
    def __del__(self):
        self.close_db()
        
    def process_kwargs(self, kwargs_dict):
        
        bracket_name = ''
        min_val = 0
        max_val = 99999999        
        
        for key in kwargs_dict:
            if key == 'bracket_name':           
                bracket_name = Hlp.stringify(kwargs_dict[key])
            if key == 'min_val':           
                min_val = Hlp.stringify(kwargs_dict[key])
            elif key == 'max_val':
                max_val = Hlp.stringify(kwargs_dict[key])

        return [bracket_name, min_val, max_val]
    
    
    def get_all_rows(self):
        
        select_stmnt = 'select * from donor_brackets'
        try:
            self._cur_.execute(select_stmnt)
            results = self._cur_.fetchall()
        except:
            self._db_.rollback()
            logging.error('Could not execute: ' + select_stmnt)
            
        return results
    
    def get_all_brackets(self):
        
        results = self.get_all_rows()
        
        brackets = list()
        
        for row in results:
            brackets.append(self.get_donor_bracket_record_field(row,'bracket_name'))
        
        return brackets
    
    def get_row_by_min_value(self, min_val, max_val):
        
        select_stmnt = 'select * from donor_brackets where min_val = %s and max_val = %s' % (str(min_val), str(max_val))
        
        try:
            self._cur_.execute(select_stmnt)
            results = self._cur_.fetchall()
        except:
            self._db_.rollback()
            logging.error('Could not execute: ' + select_stmnt)
            
        return results
    
    
    def insert_row(self, **kwargs):
        
        insert_stmnt = 'insert into donor_brackets where '
        
        bracket_name, min_val, max_val = self.process_kwargs(kwargs)
        
        val = '(' + bracket_name + ',' + min_val + ',' + max_val + ');'
                    
        insert_stmnt = insert_stmnt + val
        
        return self.execute_SQL(insert_stmnt)
        
    
    
    def delete_row(self, **kwargs):
        
        bracket_name, min_val, max_val = self.process_kwargs(kwargs)
        
        deleteStmnt = 'delete from donor_brackets where bracket_name = \'%s\' and min_val = %s and max_val = %s;' % (bracket_name, min_val, max_val)
        
        return self.execute_SQL(deleteStmnt)

    """
        This method handles mapping test row fields to col names
        
    """
    def get_donor_bracket_record_field(self, row, key):
        
        try:
            if key == 'bracket_name':
                return row[0]
            elif key == 'min_val':
                return row[1]
            elif key == 'max_val':           
                return row[2]
        
        except Exception as inst:
            
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)      # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            return ''
        
"""

    CLASS :: PageCategoryTableLoader
    
    db42.pmtpa.wmnet.rfaulk.page_category :
        
+------------+-----------------+------+-----+---------+-------+
| Field      | Type            | Null | Key | Default | Extra |
+------------+-----------------+------+-----+---------+-------+
| page_id    | int(8) unsigned | YES  | MUL | NULL    |       |
| page_title | varbinary(255)  | YES  | MUL | NULL    |       |
| category   | varbinary(255)  | YES  | MUL | NULL    |       |
+------------+-----------------+------+-----+---------+-------+

"""
class PageCategoryTableLoader(TableLoader):
    
    def __init__(self):
        
        self._db_ = MySQLdb.connect(host=projSet.__db_server_internproxy__, user=projSet.__user_internproxy__, db=projSet.__db_internproxy__, port=projSet.__db_port_internproxy__, passwd=projSet.__pass_internproxy__)
        self._cur_ = self._db_.cursor()
    
        self._top_level_categories_ = ['Mathematics',
         'People',
         'Science',
         'Law',
         'History',
         'Geography',
         'Culture',
         'Agriculture',
         'Politics',
         'Nature',
         'Technology',
         'Education',
         'Applied_sciences',
         'Health',
         'Business',
         'Belief',
         'Humanities',
         'Society',
         'Life',
         'Environment',
         'Computers',
         'Arts',
         'Language']

    def __del__(self):
        self.close_db()

    def get_article_categories_by_page_ids(self, page_id_list):
        
        page_id_str = ''
        for id in page_id_list:
            page_id_str = page_id_str + 'page_id = %s or ' % str(id)
        page_id_str = page_id_str[:-4]
        
        sql = 'select category from rfaulk.page_category where %s' % page_id_str
        
        results = self.execute_SQL(sql)
        
        category_counts = dict()
        for category in self._top_level_categories_:
            category_counts[category] = 0
            
        for row in results:
            category = row[0]
            category = category.split(',')[0]
            category_counts[category] = category_counts[category] + 1            
            
        return category_counts
    