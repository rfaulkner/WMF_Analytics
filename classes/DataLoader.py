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
import sys, MySQLdb, math, datetime, re, logging, csv, operator, numpy as np

""" Import Analytics modules """
import config.settings as projSet
import classes.QueryData as QD
import classes.TimestampProcessor as TP
import classes.Helper as Hlp
import classes.FundraiserDataHandler as FDH

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
            map_autoviv_to_list            - conert a nested dict to a list 
               
"""
class DataLoader(object):

    ONE_STEP_PATTERN = '_1S'
    BANNER_PATTERN = 'B_'
    
    """
        Constructor
        
        Ensure that the query is run for each new object
    """
    def __init__(self):
        
        self._query_names_ = dict()
        self._data_handler_ = None   # class that will define how to process the query fields
        self._query_type_ = ''       # Stores the query type (dependent on the data handler definition)
        self._results_ = None
        self._col_names_ = None
        self._was_run_ = False        
    
        self.init_db() 
           
        """ State for all new dataloader objects is set to indicate that the associated SQL has yet to be run """
        self._was_run_ = False
        
        
    def init_db(self):
        
        """ Establish connection """
        self._db_ = MySQLdb.connect(host=projSet.__db_server__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__, passwd=projSet.__pass__)
        #self._db_ = MySQLdb.connect(host=projSet.__db_server__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__)
        
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
        Recursive method that converts a nested dict into a list of rows
        
        @param data: autovivification object
        @param mapped_data: stores result, initially an empty list
        @param row: recursively stores current row contents
        
        e.g. map_autoviv_to_list({'1': {'2': 1, '3': 0}, '4': {'5' : 3, '6': 2}}, mapped_data, []) ==> mapped_data = [['1','2',1], ['1','3',0], ['4','5',3], ['4','6',2]]
    """
    def map_autoviv_to_list(self, data, mapped_data, row):
        
        if(isinstance(data, dict)):
            for elem in data:                
                new_row = row[:]
                new_row.append(elem)
                 
                self.map_autoviv_to_list(data[elem], mapped_data, new_row)
        else:
            new_row = row[:]
            new_row.append(data)
            mapped_data.append(new_row)


    """
        Return the column names from the connection cursor
        
        @return: list of column names from stored results
    """
    def get_column_names(self):
        
        column_data = self._cur_.description
        column_names = list()
        
        for elem in column_data:
            column_names.append(elem[0])
            
        return column_names
    
    
    """
        Determine one step banners for a given campaign

        INPUT:

            start_time        - start timestamp for reporting 
            end_time          - end timestamp for reporting
            campaign          - the campaign on which to select
    """
    def get_one_step_banners(self, start_time, end_time, campaign):
        
        start_time = MySQLdb._mysql.escape_string(str(start_time))
        end_time = MySQLdb._mysql.escape_string(str(end_time))
        campaign = MySQLdb._mysql.escape_string(str(campaign))
        
        sql_stmnt_1 = "(select utm_source from banner_impressions where on_minute >= '%s' and on_minute < '%s' and utm_source regexp '%s' group by 1) as bi" % (start_time, end_time, IntervalReportingLoader.ONE_STEP_PATTERN)
        sql_stmnt_2 = "(select SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner from drupal.contribution_tracking where ts >= '%s' and ts < '%s' and utm_campaign = '%s' and SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) regexp '%s' group by 1) as lp" % (start_time, end_time, campaign, IntervalReportingLoader.ONE_STEP_PATTERN)
        
        sql_stmnt = "select bi.utm_source from %s join %s on bi.utm_source = lp.banner " % (sql_stmnt_1, sql_stmnt_2)
        
        results = self.execute_SQL(sql_stmnt)
        one_step_banners = list()
        for row in results:
            one_step_banners.append(row[0])
            
        return one_step_banners


    """
        Return a specific query name given a query type
                
    """
    def stringify(self, str_to_stringify):
            
            if str_to_stringify is None:
                return 'NULL'
            
            return '"' + str_to_stringify + '"'
"""

    General Long term trends for analysis.  These queries will consist of a small set of metrics to be measured.
    The first column is a time index representing the hour
    
"""
class LongTermTrendsLoader(DataLoader):
    
    _LT_BANNER_IMPRESSIONS_ = 0
    _LT_LP_IMPRESSIONS_ = 1
    _LT_DONATIONS_ = 2
    _LT_CLICK_RATE_ = 3
    
    def __init__(self):
        
        """ Call constructor of parent """
        DataLoader.__init__(self)
        
        self._results_ = None
    
    
    """
        PROCESS OPTIONAL KWARGS
        
            Allow the minimum number of views to be specified
    """
    def process_kwargs(self, kwargs_dict):
        
        min_val = '0' # filters no campaigns
        campaign = ''
        metric_name = ''
        interval = 60
        
        """ Process keys -- Escape parameters """
        for key in kwargs_dict:
            if key == 'min_val':       
                min_val = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))                    
            elif key == 'campaign':       
                min_val = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
            elif key == 'metric_name':       
                metric_name = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))                    
            elif key == 'interval':       
                interval = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))                    
                        
        return min_val, campaign, metric_name, interval
    
    """
        Based on the query type provided execute a query
    """
    def run_query(self, start_time, end_time, query_type, **kwargs):
        
        """ Escape timestamps """
        start_time = MySQLdb._mysql.escape_string(str(start_time).strip())
        end_time = MySQLdb._mysql.escape_string(str(end_time).strip())
        
        min_val, campaign, metric_name, interval = self.process_kwargs(kwargs)
        
        if query_type == 0: 
            
            sql = "select concat(DATE_FORMAT(on_minute,'%sY%sm%sd%sH'), '0000') as hr, sum(counts) as impressions from banner_impressions where on_minute >= '%s' and on_minute < '%s' group by 1"
            sql = sql % ('%', '%', '%', '%', start_time, end_time)
            
        elif query_type == 1:
            
            sql = "select concat(DATE_FORMAT(request_time,'%sY%sm%sd%sH'), '0000') as hr, count(*) as views from landing_page_requests where request_time >= '%s' and request_time < '%s' group by 1"
            sql = sql % ('%', '%', '%', '%', start_time, end_time)
            
        elif query_type == 2:
            
            sql = "select concat(DATE_FORMAT(receive_date,'%sY%sm%sd%sH'), '0000') as hr, count(*) as donations, sum(total_amount) as amount from civicrm.civicrm_contribution where receive_date >= '%s' and receive_date < '%s' group by 1"
            sql = sql % ('%', '%', '%', '%', start_time, end_time)
            
        elif query_type == 3:
            
            sql = "select  bi.hr, views / impressions as click_rate from (select concat(DATE_FORMAT(on_minute,'%sY%sm%sd%sH'), '0000') as hr, sum(counts) as impressions from banner_impressions " + \
            "where on_minute >= '%s' and on_minute < '%s' group by 1) as bi join (select concat(DATE_FORMAT(request_time,'%sY%sm%sd%sH'), '0000') as hr, count(*) as views from landing_page_requests " + \
            "where request_time >= '%s' and request_time < '%s'  group by 1 ) as lpi on bi.hr = lpi.hr"
            sql = sql % ('%', '%', '%', '%', start_time, end_time, '%', '%', '%', '%', start_time, end_time)
        
        self._results_ = self.execute_SQL(sql)
        column_names = self.get_column_names()
        metric_index = column_names.index(metric_name)
        
        """ Normalize the data for missing hours - this should be rare given the nature of the data but is needed to be conceptually complete """
        
        counts = list()
        times = list()

        for row in self._results_:  
            
            times.append(row[0])
            counts.append(float(row[metric_index]))

        start_time_obj = TP.timestamp_to_obj(start_time[:10] + '0000', 1)
        end_time_obj = TP.timestamp_to_obj(end_time[:10] + '0000', 1)
        diff = end_time_obj - start_time_obj
        
        num_hours = diff.seconds / (interval * 60) + diff.days * 24
        
        ts_list = TP.create_timestamp_list(start_time_obj, num_hours, interval)

        new_counts = list()
        count_index = 0
        for ts in ts_list:
            if not(ts in times):                
                new_counts.append(0.0)
            else:
                new_counts.append(counts[count_index])
                count_index = count_index + 1
                
        counts = new_counts
           
        return ts_list, counts
        
"""

    This Loader inherits the functionality of DaatLoader and handles SQL queries that group data by time intervals.  These are generally preferable for most
    of the time dependent data analysis and also provides functionality that enables the raw results to be combined over all keys
    
"""
class SummaryReportingLoader(DataLoader):

    
    def __init__(self, query_type):
        
        """ Call constructor of parent """
        DataLoader.__init__(self)
        
        self._results_ = None
        
        if cmp(FDH._QTYPE_BANNER_, query_type) == 0:
            self._query_name_ = 'report_banner_metrics'
        elif cmp(FDH._QTYPE_LP_, query_type) == 0:
            self._query_name_ = 'report_LP_metrics'
        elif cmp(FDH._QTYPE_BANNER_LP_, query_type) == 0:
            self._query_name_ = 'report_bannerLP_metrics'
        elif cmp(FDH._QTYPE_TOTAL_, query_type) == 0:
            self._query_name_ = 'report_total_metrics'
    
    """
        PROCESS OPTIONAL KWARGS
        
            Allow the minimum number of views to be specified
    """
    def process_kwargs(self, kwargs_dict):
        
        min_views = '1000' # filters no campaigns
        one_step = False
        
        """ Process keys -- Escape parameters """
        for key in kwargs_dict:
            if key == 'min_views':       
                min_views = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
            elif key == 'one_step':       
                one_step = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                                
                """ Ensure the keyarg is boolean """
                if cmp(one_step, 'True') == 0:
                    one_step = True
                else:            
                    one_step = False
        
        return min_views, one_step
    
    
    def run_query(self, start_time, end_time, campaign, **kwargs):
        
        min_views, one_step = self.process_kwargs(kwargs)
        
        """ In the case that we look at one step banners the results for one step and two step pipeline totals are combined"""
        if self.get_one_step_banners(start_time, end_time, campaign) or one_step:
            
            filename = projSet.__sql_home__+ self._query_name_ + '_1S.sql'
            sql_stmnt = Hlp.file_to_string(filename)
            sql_stmnt = QD.format_query(self._query_name_, sql_stmnt, [start_time, end_time, campaign, min_views])        
        
            logging.info('Using query: ' + self._query_name_)
            results_1 = self.execute_SQL(sql_stmnt)
            
            filename = projSet.__sql_home__+ self._query_name_ + '.sql'
            sql_stmnt = Hlp.file_to_string(filename)
            sql_stmnt = QD.format_query(self._query_name_, sql_stmnt, [start_time, end_time, campaign, min_views])        

            logging.info('Using query: ' + self._query_name_)
            results_2 = self.execute_SQL(sql_stmnt)
            
            """ Combine the results from one and two step donation flows """
            aggregate_amounts = [[]]
            
            if cmp('report_total_metrics', self._query_name_) == 0:
                
                col_types = QD.get_column_types('report_total_metrics')
                col_names = self.get_column_names()
                                
                for index in range(len(col_types)):
                    
                    col_type = col_types[index]
                    col_name = col_names[index]
                    
                    if cmp(col_type, FDH._COLTYPE_KEY_) == 0:
                        
                        if results_1:
                            elem = results_1[0][index]
                        elif results_2:
                            elem = results_2[0][index]
                            
                    elif cmp(col_type, FDH._COLTYPE_AMOUNT_) == 0:
                        
                        """ If there are both one step and two step pages combine totals """
                        if results_2 and results_1:
                            elem = float(results_1[0][index]) + float(results_2[0][index])
                        elif results_1:
                            elem = float(results_1[0][index])
                        elif results_2:
                            elem = float(results_2[0][index])
                            
                    elif cmp(col_type, FDH._COLTYPE_RATE_) == 0:
                        
                        """ If there are both one step and two step pages combine totals """
                        if results_2 and results_1:
                            elem = (float(results_1[0][index]) + float(results_2[0][index])) / 2
                        elif results_1:
                            elem = float(results_1[0][index])
                        elif results_2:
                            elem = float(results_2[0][index])
                            
                    elem = QD.get_metric_data_type(col_name, elem)
                    aggregate_amounts[0].append(elem)
                    
                self._results_ = aggregate_amounts
                    
            else:
                
                results_1 = list(results_1)
                results_1.extend(list(results_2))
            
                self._results_ = results_1
            
        else:
            filename = projSet.__sql_home__+ self._query_name_ + '.sql'
            sql_stmnt = Hlp.file_to_string(filename )
            sql_stmnt = QD.format_query(self._query_name_, sql_stmnt, [start_time, end_time, campaign, min_views])        
            
            logging.info('Using query: ' + self._query_name_)
            self._results_ = self.execute_SQL(sql_stmnt)
            
            
            
    """
        GET method for query results
    """
    def get_results(self):
        
        return self._results_

    """
        Compares two artifacts from the summary for a given metric and return the winner, loser, and difference
    """
    def compare_artifacts(self, item_list, metric, **kwargs):
        
        """
            PROCESS KWARGS
        """
        if 'labels' in kwargs:
            labels = kwargs['labels']
        else:
            labels = item_list
        
        try:
            item_list = item_list[:2]
            if len(item_list) != 2:
                raise Exception()
             
        except:
            logging.error('SummaryReportingLoader::compare_artifacts() - There should be exactly two items to compare.')
            return '[Invalid items]', '[Invalid items]', 'null'
        
        """ Retrieve indices """
        key_index = QD.get_key_index(self._query_name_)
        metric_index = QD.get_metric_index(self._query_name_, metric)        
        metric_list = list()
        
        """ Get rows for given metrics - use 'label_indices' to map metrics back to labels """
        label_indices = list()
        for row in self._results_:
            if row[key_index] in item_list:
                metric_list.append(float(row[metric_index]))
                label_indices.append(item_list.index(row[key_index]))
                
        """ Compute ranking and increase """
        winning_index = metric_list.index(max(metric_list))
        winning_index = label_indices[winning_index]
        
        """ If the metric indices do not match up with the label indices swap them to synchronize the lists """
        if label_indices[0] != 0:
            tmp = metric_list[0]
            metric_list[0] = metric_list[1]
            metric_list[1] = tmp
            
        if winning_index == 1:
            losing_index = 0
        else:
            losing_index = 1
        
        winner = labels[winning_index]
        loser = labels[losing_index]
        
        percent_increase = (metric_list[winning_index] - metric_list[losing_index]) / metric_list[losing_index] * 100.0 
        percent_increase = '%5.2f' % percent_increase
        
        return winner, loser, percent_increase
    
"""

    This Loader inherits the functionality of DaatLoader and handles SQL queries that group data by time intervals.  These are generally preferable for most
    of the time dependent data analysis and also provides functionality that enables the raw results to be combined over all keys
    
"""
class IntervalReportingLoader(DataLoader):
    
    """
        Setup query list
        
    """
    def __init__(self, query_type):
                    
        """ Call constructor of parent """
        DataLoader.__init__(self)
        
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
        
        self._summary_data_ = None
    
    
    """
        PROCESS OPTIONAL KWARGS
    """
    def process_kwargs(self, kwargs_dict):
        
        one_step = False

        """ Process keys -- Escape parameters """
        for key in kwargs_dict:        
            if key == 'one_step':
                one_step = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                
                """ Ensure the keyarg is boolean """
                if cmp(one_step, 'True') == 0:
                    one_step = True
                else:            
                    one_step = False
                            
        return one_step
        
    """
        Handles both one step and two step banners
    """
    def run_query(self, start_time, end_time, interval, metric_name, campaign, **kwargs):                
        
        one_step_var = self.process_kwargs(kwargs)            
        
        query_name = self.get_sql_filename_for_query()
        logging.info('Using query: ' + query_name)

        """ 
            Determine if there are any one step banners
            
            If so execute both types of queries
        """
        if self.get_one_step_banners(start_time, end_time, campaign) or one_step_var:

            logging.info('Using one-step query...')
            metrics_1, times_1, results_1 = self.run_query_base(start_time, end_time, interval, metric_name, campaign, query_name + '_1S')
            
            self._was_run_ = False
            metrics_2, times_2, results_2 = self.run_query_base(start_time, end_time, interval, metric_name, campaign, query_name)
             
            """ Combine the results from one and two step donation flows """
            
            metrics_1.update(metrics_2)
            metrics = metrics_1
             
            times_1.update(times_2)
            times = times_1
             
            results_1 = list(results_1)
            results_1.extend(list(results_2))
            
            results = results_1
            self._results_ = results
            
        else:
            metrics, times, results = self.run_query_base(start_time, end_time, interval, metric_name, campaign, query_name)
                
        return metrics, times, results
    
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
    def run_query_base(self, start_time, end_time, interval, metric_name, campaign, query_name):
        
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
            sql_stmnt = Hlp.file_to_string(filename)
            
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

    This class inherits the IntrvalLoader functionality but utilizes the campaign DataLoader instead.  Also the results generated incorporate campaign totals also --
    the result fully specifies each campaign item (campaign - banner - landing page) and the number of views resulting.
    
    This differs from the interval reporter in that it compiles results for views and donations only for an entire donation process or pipeline.

"""
class CampaignIntervalReportingLoader(DataLoader):

    
    def __init__(self):
        
        """ Call constructor of parent """
        DataLoader.__init__(self)
        
        self._query_type_ = 'campaign'
        
        self._irl_artifacts_ = IntervalReportingLoader('campaign')
        self._irl_totals_ = IntervalReportingLoader('campaign_total')
        
        
    """
        
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
    def run_query(self, start_time, end_time, interval, metric_name, campaign, **kwargs):
        
        """ Execute the standard interval reporting query """
        data = self._irl_artifacts_.run_query(start_time, end_time, interval, metric_name, campaign, **kwargs)
        metrics = data[0] 
        times = data[1]
        
        """ Get the totals for campaign views and donations """
        data = self._irl_totals_.run_query(start_time, end_time, interval, metric_name, campaign, **kwargs)
        metrics_total = data[0] 
        times_total = data[1]
        self._results_ = data[2]
        
        """ Combine the results for the campaign totals with (banner, landing page, campaign) """
        for key in metrics_total.keys():
            metrics[key] = metrics_total[key]
            times[key] = times_total[key]
                
        return [metrics, times]

    """
        Customn for this class since it has no cursor object.
        Return the column names from the connection cursor
        
        @return: list of column names from stored results
    """
    def get_column_names(self):
        
        return ['ts', 'pipeline_name', 'views', 'donations']
        
"""

    CLASS :: CampaignReportingLoader
    
    This inherits from the DataLoader base class and handles reporting on utm_campaigns.  This reporter handles returning lists of banners and lps from a given campaign and also handles 
    reporting donation totals across all campaigns.  While this interface is concerned with campaign reporting it does not associated results with time intervals within the start and 
    end times 
      
"""
class CampaignReportingLoader(DataLoader):
    
    def __init__(self, query_type):
        
        """ Call constructor of parent """
        DataLoader.__init__(self)

        self.init_db()
        
        self._query_names_['totals'] = 'report_campaign_totals'
        self._query_names_['times'] = 'report_campaign_times'
        self._query_names_[FDH._TESTTYPE_BANNER_] = 'report_campaign_banners'
        self._query_names_[FDH._TESTTYPE_LP_] = 'report_campaign_lps'
        self._query_names_[FDH._TESTTYPE_BANNER_LP_] = 'report_campaign_bannerlps'
        
        self._query_type_ = query_type
        
            
    """ Close the connection """
    def __del__(self):
        self.close_db()
        
    """
        !! MODIFY / FIXME -- use python reflection !! ... maybe
        
        This method is retrieving campaign names 
       
        delegates the procesing to different methods
        
    """
    def run_query(self, params):
        
        data = None
        
        if self._query_type_ == 'totals':
            data = self.query_totals(params)
        elif self._query_type_ == FDH._TESTTYPE_BANNER_ or self._query_type_ == FDH._TESTTYPE_LP_ or FDH._TESTTYPE_BANNER_LP_:
            data = self.query_artifacts(params)
        
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
        sql_stmnt = Hlp.file_to_string(filename)
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
        sql_stmnt = Hlp.file_to_string(filename)        
        sql_stmnt = QD.format_query(query_name, sql_stmnt, [start_time, end_time, utm_campaign])
        
        """ Get Indexes into Query """
        key_index = QD.get_key_index(query_name)     
        
        data = list()
        
        """ Compose the data for each separate donor pipeline artifact """
        try:
            
            self._cur_.execute(sql_stmnt)
            
            results = self._cur_.fetchall()
            
            for row in results:
                if isinstance(key_index, list):
                    artifact = ''
                    for key in key_index:
                        artifact = artifact + row[key] + '-'
                    artifact = artifact[:-1]
                    data.append(artifact)
                else:
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
        Produces results for live landing page reporting using query from ../../sql/report_lp_running.sql
        
        @param start_time: start timestamp of query
        @param end_time: end timestamp of query
        
    """
    def query_live_landing_pages(self, start_time, end_time, **kwargs):
        
        CIVI_DONATE_UTM_SOURCE_DELIMETER = '~'
        ONESTEP_PATERN = '^cc[0-9][0-9]$'
        LP_NAME_FIELDS = ['Lp-layout-','appeal-template-','Appeal-','form-template-','Form-countryspecific-']
        # LP_NAME_FIELDS = ['','','','','']
        
        if 'min_donation' in kwargs:
            min_donation = kwargs['min_donation']
            
            if not(isinstance(min_donation, int)):
                min_donation = 0

        query_name = 'report_lp_running'
        
        filename = projSet.__sql_home__+ query_name + '.sql'
        
        lp_link_str_donate = 'https://donate.wikimedia.org/wiki/Special:FundraiserLandingPage?uselang=%s&' + \
        'country=%s&template=%s&appeal-template=%s&appeal=%s&form-template=%s&form-countryspecific=%s'
        
        lp_link_str_foundation = 'http://wikimediafoundation.org/wiki/%s/%s/%s'
        
        lp_link_str_1S = 'https://payments.wikimedia.org/index.php/Special:PayflowProGateway?uselang=%s&country=%s&appeal=jimmy-appeal' + \
        '&form_name=RapidHtml&ffname=webitects_2_2stepB-US&utm_medium=sitenotice&utm_source_id=%s&_cache_=true'
        
        sql_stmnt = Hlp.file_to_string(filename)
        sql_stmnt = sql_stmnt % (start_time, end_time, start_time, end_time, str(min_donation))
        
        logging.info('Using query:  report_lp_running -> get live landing pages')
                
        results =  self.execute_SQL(sql_stmnt)
        
        live_banner_link_index = QD.get_metric_index(query_name, 'live_banners')
        landing_page_index = QD.get_metric_index(query_name, 'landing_page')
        language_index = QD.get_metric_index(query_name, 'language')
        country_index = QD.get_metric_index(query_name, 'country')
        
        """ Process the new results - add links """
        new_results = list()
        for row in results:
            new_row = list()
            
            """ Filter requests from donate.wikimedia.org """
            landing_page = row[landing_page_index]
            
            if landing_page != None:
                if re.search(CIVI_DONATE_UTM_SOURCE_DELIMETER, landing_page):
                    # <template>~<appeal-template>~<appeal>~<form-template>~<form-countryspecific>
                    lp_fields = landing_page.split(CIVI_DONATE_UTM_SOURCE_DELIMETER)
                    if len(lp_fields) == 5:
                        
                        landing_page = LP_NAME_FIELDS[0] + lp_fields[0] + '~' + LP_NAME_FIELDS[1] + lp_fields[1] + '~' + LP_NAME_FIELDS[2] + lp_fields[2] + '~' +  \
                         LP_NAME_FIELDS[3] + lp_fields[3] + '~' +  LP_NAME_FIELDS[4] + lp_fields[4]

                        lp_link = lp_link_str_donate % (row[language_index], row[country_index], LP_NAME_FIELDS[0] + lp_fields[0], LP_NAME_FIELDS[1] + lp_fields[1], \
                                LP_NAME_FIELDS[2] + lp_fields[2], LP_NAME_FIELDS[3] + lp_fields[3], LP_NAME_FIELDS[4] + lp_fields[4])

                    else:
                        landing_page = 'None'
                        lp_link = '<b>Missing fields in utm_source string.</b>'
                
                elif re.search(ONESTEP_PATERN, landing_page):
                
                    utm_source_id = landing_page.split('c')[2]
                    lp_link = lp_link_str_1S % (row[language_index], row[country_index], utm_source_id)
                
                else:
                    lp_link = lp_link_str_foundation % (landing_page, row[language_index], row[country_index])
            
            else:
                lp_link = ''
            
            """ Build LP link from fields """
            
            for index in range(len(row)):
                if index == live_banner_link_index:
                    new_row.append('<a href="%s">%s</a>' % (row[index], row[language_index]))
                elif index == landing_page_index:
                    if len(lp_link) > 0:
                        new_row.append('<a href="%s">%s</a>' % (lp_link, landing_page))            
                    else:
                        new_row.append('')
                        
                elif index != language_index:
                    new_row.append(row[index])
                    
            new_results.append(new_row)
        
        columns = ['Campaign', 'Country', 'Language', 'Landing Page', 'Views', 'Donations', 'Total Amount ($)']
        
        return new_results, columns
        # columns = ['Country', 'Language', 'Live Banners', 'Landing Page', 'Views', 'Donations', 'Total Amount ($)']
        

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
        
        """ Call constructor of parent """
        DataLoader.__init__(self)
        
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
        
        start_time = MySQLdb._mysql.escape_string(str(start_time))
        end_time = MySQLdb._mysql.escape_string(str(end_time))
        campaign = MySQLdb._mysql.escape_string(str(campaign))
                                         
        query_name = self._query_names_
        logging.info('Using query: ' + query_name)
        
        """ Load the SQL File & Format """
        filename = projSet.__sql_home__ + query_name + '.sql'
        sql_stmnt = Hlp.file_to_string(filename)

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
        
        """ Escape parameters """
        degrees_of_freedom = MySQLdb._mysql.escape_string(str(degrees_of_freedom))
        t = MySQLdb._mysql.escape_string(str(t))
        
        select_stmnt = 'select max(p) from t_test where degrees_of_freedom = ' + degrees_of_freedom + ' and t >= ' + t
        
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

        """ Process keys -- Escape parameters """
        for key in kwargs_dict:
            if key == 'utm_campaign':       
                utm_campaign = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))    
                utm_campaign = self.stringify(utm_campaign)
            elif key == 'test_name':
                test_name = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                test_name = self.stringify(test_name)
            elif key == 'test_type':
                test_type = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                test_type = self.stringify(test_type)
            elif key == 'start_time':
                start_time = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                start_time = self.stringify(start_time)
            elif key == 'end_time':
                end_time = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                end_time = self.stringify(kwargs_dict[key])
            elif key == 'winner':
                winner = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                winner = self.stringify(winner)
            elif key == 'is_conclusive':
                is_conclusive = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                is_conclusive = self.stringify(is_conclusive)
            elif key == 'html_report':                             
                # html_report = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))

                html_pieces = str(kwargs_dict[key]).split('\\"')                
                html_report_new = ''
                
                """ Avoid escaping bits that are already escaped """
                for piece in html_pieces:
                    html_report_new = html_report_new + MySQLdb._mysql.escape_string(piece) + '\\"'
                html_report_new = html_report_new[:-4]
                
                html_report = self.stringify(html_report_new)
        
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
    
    
    def record_exists(self, **kwargs):
        
        utm_campaign = self.process_kwargs(kwargs)[2]
        
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
        
        utm_campaign = self.process_kwargs({'utm_campaign' : utm_campaign})[2]
        
        select_stmnt = 'select * from test where utm_campaign = ' + utm_campaign
        
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

    """
        This method returns an index to a given field key in the test table
    """
    def get_test_index(self, key):
        
        if key == 'test_name':
            return 0
        elif key == 'test_type':
            return 1
        elif key == 'utm_campaign':           
            return 2
        elif key == 'start_time':
            return 3
        elif key == 'end_time':
            return 4
        elif key == 'winner':
            return 5
        elif key == 'is_conclusive':
            return 6
        elif key == 'html_report':                
            return 7
        else:
            logging.error(self.__str__() + ': Test Table field does not exist.')
            return None

    def delete_row(self):
        return
    
    def get_column_names(self):
        
        return ['Name', 'Type', 'Campaign', 'Start Time', 'End Time', 'Winner', 'Conclusive?', 'Report']
    
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

        """ Process keys -- Escape parameters """
        for key in kwargs_dict:
            if key == 'type':
                type = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                type = self.stringify(type)
            elif key == 'log_copy_time':
                log_copy_time = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                log_copy_time = self.stringify(log_copy_time)
            elif key == 'start_time':
                start_time = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                start_time = self.stringify(start_time)
            elif key == 'end_time':
                end_time = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                end_time = self.stringify(end_time)
            elif key == 'log_completion_pct':
                log_completion_pct = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                log_completion_pct = log_completion_pct
            elif key == 'total_rows':
                total_rows = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
        
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
        
        log_copy_time = self.process_kwargs({'log_copy_time' : log_copy_time})[1]
        
        select_stmnt = 'select * from squid_log_record where log_copy_time = ' + log_copy_time
        
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
        Get verbose name for a table column
    """
    def get_verbose_column(self, col_name):
        
        if cmp(col_name, 'type') == 0:
            col_name = 'Log Type'
        elif cmp(col_name, 'log_copy_time') == 0:
            col_name = 'Time of Log Copy'
        elif cmp(col_name, 'start_time') == 0:
            col_name = 'Log Start Time'
        elif cmp(col_name, 'end_time') == 0:
            col_name = 'Log End Time'
        elif cmp(col_name, 'log_completion_pct') == 0:
            col_name = '% Complete'
        elif cmp(col_name, 'total_rows') == 0:
            col_name = 'Total Requests'
            
        return col_name
       

"""
    Dataloader class that handles custom data extraction from CiviCRM and drupal databases

"""
class CiviCRMLoader(TableLoader):
    
    def __init__(self):
        self.init_db()
    
    def __del__(self):
        self.close_db()
        
    def process_kwargs(self, kwargs_dict):
        
        utm_source = 'NULL'
                
        for key in kwargs_dict:
            if key == 'utm_source_arg':           
                utm_source = self.stringify(kwargs_dict[key])
        
        return [utm_source]
    
    """
        Extracts the portion of donations that were transacted via paypal and credit-card by banner and landing page
        
        @param start_timestamp, end_timestamp, campaign: Query parameters for civiCRM contributions
        
        @return nested dict storing portions for each method
        
    """
    def get_payment_methods(self, campaign, start_timestamp, end_timestamp):        
        
        """ Escape parameters """
        campaign = MySQLdb._mysql.escape_string(str(campaign))
        start_timestamp = MySQLdb._mysql.escape_string(str(start_timestamp))
        end_timestamp = MySQLdb._mysql.escape_string(str(end_timestamp))
        
        sql_banner =  "select " + \
                "SUBSTRING_index(substring_index(utm_source, '.', 2),'.',1) as banner, substring_index(utm_source, '.', -1) as payment_method, count(*) as counts " + \
                "from drupal.contribution_tracking left join civicrm.civicrm_contribution on contribution_tracking.contribution_id = civicrm.civicrm_contribution.id " + \
                "where receive_date >= '%s' and receive_date < '%s' and utm_campaign = '%s' " % (start_timestamp, end_timestamp, campaign) + \
                "group by 1,2 order by 1,2"
        
        sql_lp =  "select " + \
                "SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page, substring_index(utm_source, '.', -1) as payment_method, count(*) as counts " + \
                "from drupal.contribution_tracking left join civicrm.civicrm_contribution on contribution_tracking.contribution_id = civicrm.civicrm_contribution.id " + \
                "where receive_date >= '%s' and receive_date < '%s' and utm_campaign = '%s' " % (start_timestamp, end_timestamp, campaign) + \
                "group by 1,2 order by 1,2"
        
        results_banner = self.execute_SQL(sql_banner)
        results_lp = self.execute_SQL(sql_lp)
        
        banner_payment_methods = Hlp.AutoVivification()
        for row in results_banner:
            if cmp(row[1], 'pp') == 0:
                banner_payment_methods[row[0]]['Paypal'] = int(row[2])
            elif cmp(row[1], 'cc') == 0:
                banner_payment_methods[row[0]]['Credit Card'] = int(row[2])
            elif cmp(row[1], 'rpp') == 0:
                banner_payment_methods[row[0]]['Recurring'] = int(row[2])
                
        lp_payment_methods = Hlp.AutoVivification()
        for row in results_lp:
            if cmp(row[1], 'pp') == 0:
                lp_payment_methods[row[0]]['Paypal'] = int(row[2])
            elif cmp(row[1], 'cc') == 0:
                lp_payment_methods[row[0]]['Credit Card'] = int(row[2])
            elif cmp(row[1], 'rpp') == 0:
                lp_payment_methods[row[0]]['Recurring'] = int(row[2])
                
        for banner in banner_payment_methods:
            total = 0
            for payment_method in banner_payment_methods[banner]:
                total = total + banner_payment_methods[banner][payment_method]
            for payment_method in banner_payment_methods[banner]:
                banner_payment_methods[banner][payment_method] = '%5.2f' % (float(banner_payment_methods[banner][payment_method]) / float(total) * 100.0)

        for lp in lp_payment_methods:
            total = 0
            for payment_method in lp_payment_methods[lp]:
                total = total + lp_payment_methods[lp][payment_method]
            for payment_method in lp_payment_methods[lp]:
                lp_payment_methods[lp][payment_method] = '%5.2f' % (float(lp_payment_methods[lp][payment_method]) / float(total) * 100.0)
                                
        return banner_payment_methods, lp_payment_methods


    """
        Extracts the portion of converted donations by banner and landing page and language
        
        @param start_timestamp, end_timestamp, campaign: Query parameters for civiCRM contributions
        
        @return nested dict storing portions for each method
    """
    def get_donor_by_language(self, campaign, start_timestamp, end_timestamp):
        
        """ Escape parameters """
        campaign = MySQLdb._mysql.escape_string(str(campaign))
        start_timestamp = MySQLdb._mysql.escape_string(str(start_timestamp))
        end_timestamp = MySQLdb._mysql.escape_string(str(end_timestamp))
        
        sql_lp_langs = "select lang as language, utm_source as banner, landing_page, count(*) as views from faulkner.landing_page_requests " + \
        "where request_time >= %s and request_time < %s and utm_campaign = '%s' group by 1,2,3" % (start_timestamp, end_timestamp, campaign)
        
        sql_civi_langs = "select substring_index(substring_index(utm_source, '.', 2),'.',1) as banner, substring_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page, " + \
        "substring_index(substring_index(referrer, '.', 1),'/',-1) as language, count(*) as donations " + \
        "from drupal.contribution_tracking left join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id) " + \
        "where ts >= %s and ts < %s and utm_campaign = '%s' group by 1,2,3" \
        % (start_timestamp, end_timestamp, campaign)
        
        sql_all = "select tmp1.banner, tmp1.landing_page, tmp1.language, round(tmp1.donations / tmp2.views, 4) as conversion " + \
        "from (%s) as tmp1 join (%s) as tmp2 on tmp1.banner = tmp2.banner and tmp1.landing_page = tmp2.landing_page and tmp1.language = tmp2.language;" % (sql_civi_langs, sql_lp_langs)
        
        sql_totals = "select 'Total' as banner, '' as landing_page, tmp1.language, round(sum(tmp1.donations) / sum(tmp2.views), 4) as conversion " + \
        "from (%s) as tmp1 join (%s) as tmp2 on tmp1.banner = tmp2.banner and tmp1.landing_page = tmp2.landing_page and tmp1.language = tmp2.language group by 3;" % (sql_civi_langs, sql_lp_langs)
        
        results_1 = self.execute_SQL(sql_all)
        results_2 = self.execute_SQL(sql_totals)
        
        """ filter out non-language results """
        
        columns = self.get_column_names()
        filtered_results = list()
        for row in results_1:
            """ Look at the length of the language field """
            if len(row[2]) == 2:
                filtered_results.append(row)
                
        for row in results_2:
            """ Look at the length of the language field """
            if len(row[2]) == 2:
                filtered_results.append(row)
                    
        return columns, filtered_results


    """
        Returns the earliest time for a donation to the campaign
    """
    def get_earliest_donation(self, campaign):
        
        """ Escape parameters """
        campaign = MySQLdb._mysql.escape_string(str(campaign))

        sql = "select min(receive_date) as earliest_timestamp " + \
        "from drupal.contribution_tracking left join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id) " + \
        "where utm_campaign REGEXP '%s'" % campaign
        
        results = self.execute_SQL(sql)
        
        earliest_timestamp = results[0][0]
        earliest_timestamp = TP.timestamp_from_obj(earliest_timestamp, 1, 2) # Round down to the nearest minute
        
        return earliest_timestamp

    """
        Returns the latest time for a donation to the campaign
    """    
    def get_latest_donation(self, campaign):
        
        """ Escape parameters """
        campaign = MySQLdb._mysql.escape_string(str(campaign))

        sql = "select max(receive_date) as latest_timestamp " + \
        "from drupal.contribution_tracking left join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id) " + \
        "where utm_campaign REGEXP '%s'" % campaign
        
        results = self.execute_SQL(sql)
        
        latest_timestamp = results[0][0]
        latest_timestamp = latest_timestamp + datetime.timedelta(minutes=1) # Round up to the nearest minute
        latest_timestamp = TP.timestamp_from_obj(latest_timestamp, 1, 2)
        
        return latest_timestamp

    """
        Returns a list of campaigns running within a given interval
    """    
    def get_campaigns_in_interval(self, start_time, end_time, **kwargs):

        """ Check for a campaign filter """
        
        if 'campaign_filter' in kwargs:
            campaign_filter = kwargs['campaign_filter']
            if not(isinstance(campaign_filter, str)):
                campaign_filter = ''
            else:
                campaign_filter = MySQLdb._mysql.escape_string(str(campaign_filter))
                
        """ Escape parameters """
        start_time = MySQLdb._mysql.escape_string(str(start_time).strip())
        end_time = MySQLdb._mysql.escape_string(str(end_time).strip())
        
        sql = "select utm_campaign " + \
        "from drupal.contribution_tracking left join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id) " + \
        "where ts >= '%s' and ts < '%s' and utm_campaign regexp '%s' group by 1" % (start_time, end_time, campaign_filter)
        
        results = self.execute_SQL(sql)
        
        campaigns = list()
        for row in results:
            campaigns.append(str(row[0]))
        
        return campaigns
                
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
                
        """ Process kwargs - Escape parameters """                
        for key in kwargs_dict:
            if key == 'utm_source_arg':
                utm_source = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))           
                utm_source = self.stringify(utm_source)
            elif key == 'referrer_arg':
                referrer = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                referrer = self.stringify(referrer)
            elif key == 'country_arg':
                country = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                country = self.stringify(country)
            elif key == 'lang_arg':
                lang = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                lang = self.stringify(lang)
            elif key == 'counts_arg':
                counts = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                counts = self.stringify(counts)
            elif key == 'on_minute_arg':
                on_minute = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
            elif key == 'start_timestamp_arg':
                start_timestamp = MySQLdb._mysql.escape_string(str(start_timestamp))
        
        return [start_timestamp, utm_source, referrer, country, lang, counts, on_minute]
    
    
    def insert_row(self, **kwargs):
        
        insert_stmnt = 'insert into banner_impressions values '
        
        start_timestamp, utm_source, referrer, country, lang, counts, on_minute = self.process_kwargs(kwargs)
        
        on_minute = "convert('" + on_minute + "00', datetime)"
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
        
        start_timestamp = self.process_kwargs({ 'start_timestamp_arg' : start_timestamp})[0]
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
        start_timestamp = 'NULL' 
        timestamp = 'NULL'
        
        for key in kwargs_dict:
            if key == 'utm_source_arg':   
                utm_source = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))        
                utm_source = self.stringify(utm_source)
            elif key == 'utm_campaign_arg':
                utm_campaign = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                utm_campaign = self.stringify(utm_campaign)
            elif key == 'utm_medium_arg':
                utm_medium = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                utm_medium = self.stringify(utm_medium)
            elif key == 'landing_page_arg':
                landing_page = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                landing_page = self.stringify(landing_page)
            elif key == 'page_url_arg':
                page_url = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                page_url = self.stringify(page_url)
            elif key == 'referrer_url_arg':
                referrer_url = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                referrer_url = self.stringify(referrer_url)
            elif key == 'browser_arg':
                browser = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                browser = self.stringify(browser)
            elif key == 'lang_arg':
                lang = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                lang = self.stringify(lang)
            elif key == 'country_arg':
                country = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                country = self.stringify(country)
            elif key == 'project_arg':
                project = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                project = self.stringify(project)
            elif key == 'ip_arg':
                ip = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                ip = self.stringify(ip)
            elif key == 'start_timestamp_arg':
                start_timestamp = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                start_timestamp = self.stringify(start_timestamp)
            elif key == 'timestamp_arg':
                timestamp = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                timestamp = self.stringify(timestamp)
                
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
        
        start_timestamp = self.process_kwargs({'start_timestamp_arg' : start_timestamp})[0]
        deleteStmnt = 'delete from landing_page_requests where start_timestamp = \'' + start_timestamp + '\';'
        
        return self.execute_SQL(deleteStmnt)

    """
        Retrieve the page ids for referrers from a given log start timestamp
    """
    def get_lp_referrers_by_log_start_timestamp(self, start_timestamp, campaign):
        
        params = self.process_kwargs({'start_timestamp_arg' : start_timestamp, 'utm_campaign_arg' : campaign})
        start_timestamp = params[0]
        campaign = params[3]
        
        select_stmnt = "select referrer_url from landing_page_requests where start_timestamp = %s and utm_campaign = %s;" % (start_timestamp, campaign)
        referrer_urls = list()
        
        results = self.execute_SQL(select_stmnt)
        
        for row in results:
            referrer_urls.append(row[0])

        return self.get_referrers(referrer_urls)
    
    """
       Retrieve the page ids for a list of referrer urls  
    """
    def get_referrers(self, referrer_urls):
                
        select_stmt_page_ids = 'select page_id, page_title from page where%sand page_namespace = 0;'
        
        referrers = list()
        ref_ids = list()
                
        """ Determine if the urls originate at some article - Get the referrers title """
        page_title_str = ' ('
        for ref_url in referrer_urls:
            if (re.search('wikipedia.org/wiki/', ref_url)):
                ref_title = ref_url.split('wikipedia.org/wiki/')[1]
                # referrers.append(ref_title)
                
                page_title_str = page_title_str + 'page_title = %s or ' % self.stringify(MySQLdb._mysql.escape_string(ref_title))
                
        if len(page_title_str) > 2:
            page_title_str = page_title_str[:-4] + ') '
        else:
            return [], []
                
        """ Connect to 'enwiki' """    
        #self.establish_enwiki_conn()
        
        """ Get the page ids for the referrers """
        results = self.execute_SQL(select_stmt_page_ids % page_title_str)
        
        for row in results:
            ref_ids.append(int(row[0]))
            referrers.append(row[1])
            
        """ Restore connection to 'faulkner' """
        #self.establish_faulkner_conn()
        
        return ref_ids, referrers
    
    """
        Returns the timestamp start keys appearing within a timeframe
        
        @param start_time:  timestamp string of the start time
        @param end_time:  timestamp string of the end time     
    """
    def get_log_start_times(self, start_time, end_time):
        
        timestamps = list()
        
        start_time = MySQLdb._mysql.escape_string(str(start_time))
        end_time = MySQLdb._mysql.escape_string(str(end_time))
        
        sql = "select distinct(start_timestamp) from landing_page_requests where request_time >= '%s' and request_time < '%s'" % (start_time, end_time)
        results = self.execute_SQL(sql)
        
        for row in results:
            timestamps.append(row[0])
        
        return timestamps
        
    """
        Returns the earliest timestamp for a given campaign
        
        @param utm_campaign:  campaign to evaluate
    """
    def get_earliest_campaign_view(self, utm_campaign):
        
        utm_campaign = MySQLdb._mysql.escape_string(str(utm_campaign))
        
        sql = "select min(request_time) from landing_page_requests where utm_campaign = '%s'" % utm_campaign
        results = self.execute_SQL(sql)
        
        earliest_timestamp = results[0][0]
        earliest_timestamp = TP.timestamp_from_obj(earliest_timestamp, 1, 2)
        
        return earliest_timestamp
    
    """
        Returns the latest timestamp for a given campaign
        
        @param utm_campaign:  campaign to evaluate
    """
    def get_latest_campaign_view(self, utm_campaign):
        
        utm_campaign = MySQLdb._mysql.escape_string(str(utm_campaign))
        
        sql = "select max(request_time) from landing_page_requests where utm_campaign = '%s'" % utm_campaign
        results = self.execute_SQL(sql)
        
        latest_timestamp = results[0][0]
        latest_timestamp = latest_timestamp + datetime.timedelta(minutes=1) # Round up to the nearest minute
        latest_timestamp = TP.timestamp_from_obj(latest_timestamp, 1, 2)
        
        return latest_timestamp

    """
        Counts the number of views for a given campaign
    """
    def get_campaign_view_count(self, utm_campaign):
        
        utm_campaign = MySQLdb._mysql.escape_string(str(utm_campaign))
        
        sql = "select count(*) from landing_page_requests where utm_campaign regexp '%s'" % utm_campaign
        results = self.execute_SQL(sql)
        
        return int(results[0][0])
    
    
    """
        Counts the number of views for a given campaign
    """
    def is_one_step(self, start_time, end_time, campaign_filter):
        
        campaign_filter = MySQLdb._mysql.escape_string(str(campaign_filter))
        
        sql = "select ecomm.utm_campaign, ecomm.landing_page, if(lp.views, lp.views, 0) as lp_views, if(ecomm.views, ecomm.views, 0) as ecomm_views from " + \
        "(select utm_campaign, landing_page, count(*) as views from landing_page_requests where request_time > '%s' and request_time <= '%s' and utm_campaign regexp '%s' group by 1,2) as lp right join " + \
        "(select utm_campaign, SUBSTRING_index(substring_index(utm_source, '.', 2),'.',-1) as landing_page, count(*) as views from drupal.contribution_tracking left join civicrm.civicrm_contribution on (drupal.contribution_tracking.contribution_id = civicrm.civicrm_contribution.id) " + \
        "where receive_date > '%s' and receive_date <= '%s' and utm_campaign regexp '%s' group by 1,2) as ecomm on lp.utm_campaign = ecomm.utm_campaign and lp.landing_page = ecomm.landing_page " + \
        "having lp_views < ecomm_views and ecomm_views > 10" 
        
        sql = sql % (start_time, end_time, campaign_filter, start_time, end_time, campaign_filter) 

        results = self.execute_SQL(sql)

        if len(results) == 0:
            return False
        else:
            return True
        
        
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
        
        """ Compute IP number """
        try:
            ip_fields = ip_string.split('.')
            w = int(ip_fields[0])
            x = int(ip_fields[1])
            y = int(ip_fields[2])
            z = int(ip_fields[3])
            
            ip_num = 16777216 * w + 65536 * x + 256 * y + z;
            
            sql_stmnt = 'select country_ISO_1 from ip_country where ' + str(ip_num) + ' >= ip_from and ' + str(ip_num) + ' <= ip_to'
        
        except:
            
            logging.error('Country could not be localized from IP address string (formatting).')
            return 'None'
        
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
                
                vals = MySQLdb._mysql.escape_string('(\'' + vals + '\')')
                sql_stmnt = insert_stmnt + vals
                
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
                pattern_type = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))     
                pattern_type = self.stringify(pattern_type)
            elif key == 'pattern':
                pattern = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                pattern = self.stringify(pattern)
                
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
        
        pattern_type, pattern = self.process_kwargs({'pattern_type' : pattern_type, 'pattern' : pattern})
        
        deleteStmnt = 'delete from mining_patterns where pattern_type = %s and pattern = %s;' % (pattern_type, pattern)
        
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
                bracket_name = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))           
                bracket_name = self.stringify(bracket_name)
            if key == 'min_val':         
                min_val = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))  
                min_val = self.stringify(min_val)
            elif key == 'max_val':
                max_val = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                max_val = self.stringify(max_val)

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
        
        params = self.process_kwargs({'max_val' : max_val, 'min_val' : min_val})
        max_val = params[2]
        min_val = params[1]
        
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
        
+----------------+-----------------+------+-----+---------+-------+
| Field          | Type            | Null | Key | Default | Extra |
+----------------+-----------------+------+-----+---------+-------+
| page_id        | int(8) unsigned | YES  | MUL | NULL    |       |
| page_title     | varbinary(255)  | YES  | MUL | NULL    |       |
| category       | varbinary(255)  | YES  |     | NULL    |       |
| category_value | varbinary(255)  | YES  | MUL | NULL    |       |
+----------------+-----------------+------+-----+---------+-------+

"""
class PageCategoryTableLoader(TableLoader):
    
    def __init__(self):
        
        self.init_db()
    
        self._top_level_categories_ = ['Mathematics',
         'People',
         'Science',
         'Law',
         'History',
         'Culture',
         'Politics',
         'Technology',
         'Education',
         'Health',
         'Business',
         'Belief',
         'Humanities',
         'Society',
         'Life',
         'Environment',
         'Computers',
         'Arts',
         'Language',
         'Places']

    def __del__(self):
        self.close_db()

    """
        Retrieve article top-level category for a list of page ids
        
        @param page_id_list: list of ids from page table
    """
    def get_article_categories_by_page_ids(self, page_id_list):
        
        page_id_str = ''
        for id in page_id_list:
            page_id_str = page_id_str + 'page_id = %s or ' % str(id)
        page_id_str = page_id_str[:-4]
        
        page_id_str = MySQLdb._mysql.escape_string(page_id_str)
        sql = 'select category from faulkner.page_category where %s' % page_id_str
        
        results = self.execute_SQL(sql)
        
        category_counts = dict()
        for category in self._top_level_categories_:
            category_counts[category] = 0
            
        for row in results:
            category = row[0]
            category = category.split(',')[0]
            category_counts[category] = category_counts[category] + 1            
            
        return category_counts
    
    
    """
        Produce the weighting for each category based on vector representations
        
        @param page_id_list: list of ids from page table
    """
    def get_article_vector_counts(self, page_id_list):
        
        page_id_str = ''
        for id in page_id_list:
            page_id_str = page_id_str + 'page_id = %s or ' % str(id)
        page_id_str = page_id_str[:-4]
        
        page_id_str = MySQLdb._mysql.escape_string(page_id_str)
        sql = 'select category_value from faulkner.page_category where %s' % page_id_str
        
        results = self.execute_SQL(sql)
        
        category_counts = dict()
        for category in self._top_level_categories_:
            category_counts[category] = 0.0
            
        for row in results:
            category_vector = row[0]
            
            """ Process the category_value string 
                e.g. '0.05508 0.08098 0.05508 0.03968 0.05508 0.08098 0.06749 0.06749 0.03968 0.01367 0.05508 0.03968 0.08403 0.00283 0.0 0.01367 0.02814 0.07163 0.09752 0.05209' """
            category_vector = category_vector.split()            
            
            """ Some vectors may have nan values due to missing categories - this has a fairly low prevalence """
            if 'nan' in category_vector:
                # weight = 1.0 / len(self._top_level_categories_)
                weight = 0.0
                for category in self._top_level_categories_:
                    category_counts[category] = category_counts[category] + weight
            else:
                
                category_vector = np.array(category_vector)     # convert to a numpy array]
                first_index = np.argmax(category_vector)
                np.delete(category_vector, first_index)
                second_index = np.argmax(category_vector)
                
                first_cat = self._top_level_categories_[first_index]
                second_cat = self._top_level_categories_[second_index]
                
                category_counts[first_cat] = category_counts[first_cat] + 1.0
                category_counts[second_cat] = category_counts[second_cat] + 0.0
                """
                # HERE Full vector counts are use
                for i in range(len(category_vector)):
                    category = self._top_level_categories_[i]
                    category_counts[category] = category_counts[category] + float(category_vector[i])
                """
        return category_counts
    
    """
        Computes relative portions of categories among all pages and then compares those amounts to the relative persistence of categories
        among a sample of pages.  A category score is computed from this.
        
        @param page_id_list: list of ids from page table
    """
    def get_normalized_category_counts(self, page_id_list):
        
        norm_results = NormalizedCategoryScoresTableLoader().get_all_rows()        
        norm_cats = dict()
        
        for row in norm_results:
            category = NormalizedCategoryScoresTableLoader().get_record_field(row, 'category')
            portion = NormalizedCategoryScoresTableLoader().get_record_field(row, 'portion')
            norm_cats[category] = portion
        
        category_counts = self.get_article_vector_counts(page_id_list)
        cat_count_total = 0.0
        
        for category in category_counts:
            cat_count_total = cat_count_total + category_counts[category]
        for category in category_counts:
            category_counts[category] = float(category_counts[category]) / cat_count_total

        category_score = dict()
        for category in norm_cats:
            try:
                category_score[category] = (category_counts[category] - norm_cats[category]) / norm_cats[category] * 100.0
            except:
                category_score[category] = -1.0
                pass
            
        return category_score
    
    
"""

    CLASS :: TrafficSamplesTableLoader
    
    db42.pmtpa.wmnet.rfaulk.traffic_samples:
        
    +--------------+------------------+------+-----+-------------------+-----------------------------+
    | Field        | Type             | Null | Key | Default           | Extra                       |
    +--------------+------------------+------+-----+-------------------+-----------------------------+
    | page_id      | int(10) unsigned | YES  |     | NULL              |                             |
    | page_title   | varbinary(255)   | YES  |     | NULL              |                             |
    | request_time | timestamp        | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
    +--------------+------------------+------+-----+-------------------+-----------------------------+

"""

class TrafficSamplesTableLoader(TableLoader):
    
    CREATE_TABLE = 'create table traffic_samples (page_id int(10) unsigned, page_title varbinary(255), request_time timestamp);'
    DROP_TABLE = 'drop table traffic_samples;'
    CREATE_IDX_1 = "create index idx_page_id on rfaulk.traffic_samples (page_id);"
    CREATE_IDX_2 = "create index idx_page_title on rfaulk.traffic_samples (page_title);"
    CREATE_IDX_3 = "create index idx_request_time on rfaulk.traffic_samples (request_time);"
    
    def __init__(self):    
        self.init_db()
        
    def __del__(self):
        self.close_db()

    def process_kwargs(self, kwargs_dict):
        
        page_id = -1
        page_title = self.stringify('')
        request_time = self.stringify('00000000000000')
        
        for key in kwargs_dict:
            if key == 'page_id':           
                page_id = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                page_id = page_id
            if key == 'page_title':           
                page_title = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                page_title = self.stringify(page_title)
            elif key == 'request_time':
                request_time = MySQLdb._mysql.escape_string(str(kwargs_dict[key]))
                request_time = self.stringify(request_time)

        return page_id, page_title, request_time
    
    """
        Insert a banner impression referrer sample
    """
    def insert_row(self, **kwargs):
        
        insert_stmnt = 'insert into traffic_samples values %s'
        
        page_id, page_title, request_time = self.process_kwargs(kwargs)
        
        val = '(' + page_id + ',' + page_title + ',' + 'convert(' + request_time + ', datetime)' + ');'
        
        insert_stmnt = insert_stmnt % val
        
        return self.execute_SQL(insert_stmnt)

    """
        Insert a banner impression referrer sample
    """
    def insert_multiple_rows(self, page_ids, page_titles, request_times):
        
        insert_stmnt = 'insert into traffic_samples values %s'
        num_rows = len(page_ids)
        
        val = ''
        for i in range(num_rows):
            
            page_ids[i] = MySQLdb._mysql.escape_string(str(page_ids[i]))
            page_titles[i] = MySQLdb._mysql.escape_string(str(page_titles[i])) 
            request_times[i] = MySQLdb._mysql.escape_string(str(request_times[i]))
            
            val = val + '(' + page_ids[i].__str__() + ',"' + page_titles[i] + '",' + 'convert(' + request_times[i] + ', datetime)' + '), '
        val = val[:-2]
         
        insert_stmnt = insert_stmnt % val
        
        return self.execute_SQL(insert_stmnt)


"""

    CLASS :: NormalizedCategoryScoresTableLoader
    
    db42.pmtpa.wmnet.rfaulk.normalized_category_scores:
            
    +----------------+----------------+------+-----+---------+-------+
    | Field          | Type           | Null | Key | Default | Extra |
    +----------------+----------------+------+-----+---------+-------+
    | category       | varbinary(255) | YES  |     | NULL    |       |
    | category_total | bigint(21)     | NO   |     | 0       |       |
    | portion        | decimal(25,6)  | YES  |     | NULL    |       |
    +----------------+----------------+------+-----+---------+-------+


"""

class NormalizedCategoryScoresTableLoader(TableLoader):
    
        
    CREATE_TABLE = "create table normalized_category_scores " + \
    "select category, category_total, round(category_total/total, 6) as portion " + \
    "from " + \
    "(select substring_index(category,',',1) as category, count(*) as category_total from page_category join traffic_samples on page_category.page_id = traffic_samples.page_id group by 1) as tmp1, " + \
    "(select count(*) as total from traffic_samples) as tmp2;"
    
    DROP_TABLE = 'drop table normalized_category_scores;'

    def __init__(self):    
        self.init_db()
        
    def __del__(self):
        self.close_db()
    
    """ Retrieve all rows """
    def get_all_rows(self):
        sql = 'select * from normalized_category_scores'
        return self.execute_SQL(sql)
    
    """ Retrieve the relative frequency of a given category """
    def get_category_portion(self, category):
        
        category = MySQLdb._mysql.escape_string(str(category))
        
        sql = "select portion from normalized_category_scores where category = '%s'" % category
        results = self.execute_SQL(sql)
        return float(results[2])
    
    """ Retrieve the count of traffic samples for a given category """
    def get_category_count(self, category):
        
        category = MySQLdb._mysql.escape_string(str(category))
        
        sql = "select category_total from normalized_category_scores where category = '%s'" % category
        results = self.execute_SQL(sql)
        return int(results[1])
    
    """ This method handles mapping test row fields to col names """
    def get_record_field(self, row, key):
        
        try:
            if key == 'category':
                return row[0]
            elif key == 'category_total':
                return int(row[1])
            elif key == 'portion':           
                return float(row[2])
        
        except Exception as inst:
            
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)      # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            return ''
        