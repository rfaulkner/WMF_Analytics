

"""

    This module is used to define the reporting methodologies on different types of data.  The base class
    DataReporting is defined to outline the general functionality of the reporting architecture and 
    functionality which includes generating the data via a dataloader object and transforming the data
    among different reporting mediums including matlab plots (primary medium) and html tables.
    
    The DataLoader class decouples the data access of the reports using the Adapter structural pattern.
    
    The DataReporter functions under the following assumptions about the data format:
       
       -> each set of data points has a label
       -> labels in counts and times match
       -> all time labels match
       -> time data is sequential and relative (typically denoting minutes from a reference time)
       
        e.g.
        _counts_ = {'artifact_1' : [d1, d2, d3], 'artifact_2' : [d4, d5, d6]}
        _times_ = {'artifact_1' : [t1, t2, t3], 'artifact_2' : [t1, t2, t3]}

"""

__author__ = "Ryan Faulkner"
__revision__ = "$Revision$"
__date__ = "December 16th, 2010"


""" Import python base modules """
import matplotlib
matplotlib.use('Agg') # disable plotting in the backend
import sys, pylab, math, logging, matplotlib.pyplot as plt, re


""" Import Analytics modules """
import classes.QueryData as QD
import classes.TimestampProcessor as TP
import classes.DataLoader as DL
from classes.HypothesisTest import TTest
import classes.FundraiserDataHandler as FDH
import classes.DataFilters as DF


""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

standard_metric_names = {'imp' : 'Bi', 'impressions' : 'Bi', 'views' : 'LPi', 'don_per_imp' : 'D / Bi', 'don_per_view' : 'D / LPi', 'amt_per_imp' : 'A / Bi', 'amt_per_view' : 'A / LPi', \
                         'amt_norm_per_imp' : 'An / Bi', 'amt_norm_per_view' : 'An / LPi', 'click_rate' : 'LPi / Bi', 'avg_donation' : 'AVG A', 'avg_donation_norm' : 'AVG An', \
                         'campaign' : 'Campaign', 'utm_campaign' : 'Campaign', 'utm_source' : 'B', 'banner' : 'B', 'landing_page' : 'LP', 'donations' : 'D', 'clicks' : 'Clk', 'amount' : 'A', \
                         'amount_normal' : 'An', 'country' : 'Country', 'dt_day' : 'Day'}


"""

    BASE CLASS :: DataReporting
    
    Base class for reporting fundraiser analytics.  The DataReporting classes are meant to handle DataLoader objects that pull data from MySQL
    and format the dictionaries for reporting in the form of plotting and tables.
    
    Methods that are intended to be extended in derived classes include:
    
    METHODS:
    
        run_query                 - format and execute the query to obtain data
        gen_plot                 - plots the results of the report
        write_to_html_table     - writes the results to an HTML table
        run

"""
class DataReporting(object):    
    
    """
        Exception class to handle missing query type on ConfidenceReporting obj creation
    """
    class KwargDefError(Exception):
        
        def __init__(self, value):
            self.value = value
        
        def __str__(self):
            return repr(self.value)


    def __init__(self, **kwargs):
        
        """ Configure tunable reporting params  """
        self._use_labels_= False
        self._fig_file_format_ = 'png'
        self._plot_type_ = 'line'
        self._item_keys_ = list()
        self._file_path_ = './'
        self._generate_plot_ = True

        self._font_size_ = 24
        self._fig_width_pt_ = 246.0                  # Get this from LaTeX using \showthe\columnwidth
        self._inches_per_pt_ = 1.0/72.27             # Convert pt to inch
    

        """ CLASS MEMBERS: Store the results of a query """
        self.data_loader_ = None
        self._table_html_ = ''       # Stores the table html
        self._data_plot_ = None      # Stores the plot object

        self._counts_ = dict()
        self._times_ = dict()
            
        self._set_filters()
        
        for key in kwargs:
            
            if key == 'font_size':
                self._font_size_ = kwargs[key]
            elif key == 'fig_width_pt':
                self._fig_width_pt_ = kwargs[key]
            elif key == 'inches_per_pt':
                self._inches_per_pt_ = kwargs[key]
            elif key == 'use_labels':
                self._use_labels_ = kwargs[key]
            elif key == 'fig_file_format':
                self._fig_file_format_ = kwargs[key]
            elif key == 'plot_type':                
                self._plot_type_ = kwargs[key]
            elif key == 'item_keys':                
                self._item_keys_ = kwargs[key]
            elif key == 'file_path':                
                self._file_path_ = kwargs[key]
            elif key == 'generate_plot':                
                self._generate_plot_ = kwargs[key]
            
        
    """
        Initialize the filters for data post processing
    """
    def _set_filters(self):
        
        logging.info('Initializing filters.')
        
        self._filters_ = list()
        self._filters_.append(DF.TotalCountFilter(lower_bound=-1,mutable_obj=self))
        self._filters_.append(DF.MatchKeysDataReporterFilter(mutable_obj=self))

    """
        Add new filters at run time - ensure there are no duplicates in the list
    """
    def add_filters_runtime(self, **kwargs):
        
        """  Check Filter list """
        contains_timeseries_filter = False
        for filter in self._filters_:
            if isinstance(filter, DF.TimeSeriesDataFilter):
                contains_timeseries_filter = True

        if kwargs['time_series'] and not(contains_timeseries_filter):    
            
            artifact_keys_var = kwargs['artifact_keys']
            interval_var = kwargs['interval']
                        
            self._filters_.append(DF.TimeSeriesDataFilter(artifact_keys=artifact_keys_var, interval=interval_var, mutable_obj=self))


    """
        Runs through he list of data reporting filters and executes each
    """        
    def _execute_filters(self):
        
        for filter in self._filters_:
            filter.execute()
        
    """
        These methods expose the reporting data
    """
    def get_counts(self):
        return self._counts_
    
    def get_times(self):
        return self._times_
    
    def set_counts(self, new_dict):
        self._counts_ = new_dict
    
    def set_times(self, new_dict):
        self._times_ = new_dict

    """
    
        To be overloaded by subclasses for different plotting behaviour
        
        INPUT:
                values               - a list of datetime objects
                window_length       - indicate whether the list counts back from the end
            
        RETURN: 
                return_status        - integer, 0 indicates un-exceptional execution
    
    """
    def _gen_plot(self,x, y_lists, labels, title, xlabel, ylabel, subplot_index, fname):
        return 0

    
    """
    
        General method for constructing html tables
        May be overloaded by subclasses for writing tables - this functionality currently exists outside of this class structure (test_reporting.py)
        
        INPUT:
                values               - a list of datetime objects
                window_length       - indicate whether the list counts back from the end
            
        RETURN: 
                return_status        - integer, 0 indicates un-exceptional execution
    
    """
    def _write_html_table(self, data, column_names, **kwargs):
        
        """ PROCESS KWARGS"""
        column_colours = dict()
        column_colours_idx = dict()
        
        """ 
            In the case that the markup was formatted before this method was called modify the flag 
        """
        if 'omit_cell_markup' in kwargs.keys():
            omit_cell_markup = kwargs['omit_cell_markup']
        else:
            omit_cell_markup = False
            
        if 'use_standard_metric_names' in kwargs.keys():
            column_names = self.get_standard_metrics_list(column_names)        
        
        if 'coloured_columns' in kwargs.keys():
                        
            column_colours = kwargs['coloured_columns']            
                            
            """ Assume kwargs['coloured_columns'] is a dictionary """
            try:
                
                """ Map column names if standard names are used """
                if 'use_standard_metric_names' in kwargs.keys():
                    new_column_colours = dict()
                    for col_name in column_colours:
                        new_column_colours[standard_metric_names[col_name]] = column_colours[col_name]
                    column_colours = new_column_colours
                
                for col_name in column_colours:                    
                    column_colours_idx[column_names.index(col_name)] = column_colours[col_name]
                    
            except:
                column_colours = {}
                logging.error('Could not properly process column colouring.')
                pass

                    
        html = '<table border=\"1\" cellpadding=\"10\">'
        
        """ Build headers """
        if len(column_names) > 0:
            html = html + '<tr>'            
            
            for name in column_names:
                
                if name in column_colours:
                    html = html + '<th style="background-color:' + column_colours[name] + ';">' + name + '</th>'
                else:
                    html = html + '<th>' + name.__str__() + '</th>'
            html = html + '</tr>'
        
        """ Build rows """
        for row in data:            
            html = html + self._write_html_table_row(row, coloured_columns=column_colours_idx, omit_cell_markup=omit_cell_markup)
                 
        html = html + '</table>'        
        
        return html
    
    """
        Compose a single table row - used by _write_html_table
    """
    def _write_html_table_row(self, row, **kwargs):
        
        """ 
            In the case that the markup was formatted before this method was called modify the flag 
        """        
        if 'omit_cell_markup' in kwargs.keys():
            omit_cell_markup = kwargs['omit_cell_markup']
        else:
            omit_cell_markup = False
            
            
        if 'coloured_columns' in kwargs.keys():
            column_colours_idx = kwargs['coloured_columns']
        else:
            column_colours_idx = {}

        html = '<tr>'
        idx = 0 

        for item in row: 
              
            if omit_cell_markup:
                html = html + item.__str__()
            elif idx in column_colours_idx:
                html = html + '<td style="background-color:' + column_colours_idx[idx] + ';">' + item.__str__() + '</td>'
            else:
                html = html + '<td>' + item.__str__() + '</td>'
            
            idx = idx + 1
        
        html = html + '</tr>'
        
        return html
    
    
        
    """
        Enable table generation from data formatted a list of rows
    """
    def write_html_table_from_rowlists(self, data, column_names, key_type):
        
        html = '<table border=\"1\" cellpadding=\"10\"><tr>'
        
        """ mapped data stores autovivification structure as a list of rows """
        
        """ Build headers """
        html = html + '<th>' + key_type + '</th>'
        for name in column_names:
            html = html + '<th>' + name + '</th>'
        html = html + '</tr>'
        
        """ Build rows """
        for row in data:
            html = html + '<tr>'
            for item in row:                                    
                html = html + '<td>' + item + '</td>'
            html = html + '</tr>'
        
        html = html + '</table>'        
        
        return html
    
    """
        Map general metrics to standard ones
            
        @param keys: list of column names
        @type keys: list standard metric names    
    
    """
    def get_standard_metrics_list(self, metrics_list):
        
        standard_list = list()
         
        for metric in metrics_list:
            try:
                standard_list.append(standard_metric_names[metric])
            except:
                standard_list.append(metric)
                logging.error('Metric standard name not found: %s' % metric)
                    
        
        return standard_list
    
    """
        Map general metrics to standard ones
            
        @param keys: list of column names
        @type keys: list standard metric names    
    
    """
    def get_standard_metrics_legend(self):
        
        column_names = ['<b>Metric Name</b>']
        rows = [['<b>Standard Name</b>']]
        
        for key in standard_metric_names:            
            metric_full_name = QD.get_metric_full_name(key)
            
            """ Ensure there are no repeat columns """
            if not(metric_full_name in column_names) and not(standard_metric_names[key] in rows[0]): 
                column_names.append(metric_full_name)
                rows[0].append(standard_metric_names[key])
                
        return self._write_html_table(rows, column_names) 

    """
    
        The access point of DataReporting and derived objects.  Will be used for executing and orchestrating the creation of plots, tables etc.
        To be overloaded by subclasses 
        
        INPUT:
             
        RETURN: 
                return_status        - integer, 0 indicates un-exceptional execution
                
    """
    def run(self):
        return
        
    """

    Helper method that formats Reporting data (for consumption by javascript in live_results/index.html)
    
        @param ir: Interval Reporting object
        @param pattern: A set of regexp patterns on which data keys are matched to filter
        @param empty_data: A set of empty data to be used in case there is no usable data from the reporting object 
        
        @return: dictionary storing data for javascript processing
    
    """
    def get_data_lists(self, patterns, empty_data):
        
        """ Get metrics """
        data = list()
        labels = '!'
        counts = list()
        max_data = 0.0
        min_data = 0.0
        
        """ Find the key with the highest count
        max = 0.0
        min = 0.0
        for key in self._counts_.keys():
            val_max = max(self._counts_[key]) * 1.5
            val_min = min(self._counts_[key]) * 1.5
            if val_max > max:
                max = val_max
            if val_min < min:
                min = val_min
        """
        
        """ Only add keys with enough counts """
        data_index = 0
        for key in self._counts_.keys():
            
            isFormed = False
            for pattern in patterns:
                if key == None:
                    isFormed = isFormed or re.search(pattern, '')
                else:
                    isFormed = isFormed or re.search(pattern, key)
                    
            # if sum(self._counts_[key]) > 0.01 * max and isFormed:
            if isFormed:
                data.append(list())
                
                if key == None or key == '':
                    labels = labels + 'empty?'
                else:
                    labels = labels + key + '?'
                
                counts.append(len(self._counts_[key]))  
                
                for i in range(counts[data_index]):
                    data[data_index].append([self._times_[key][i], self._counts_[key][i]])
                    if self._counts_[key][i] > max_data:
                        max_data = self._counts_[key][i]
                    if self._counts_[key][i] < min_data:
                        min_data = self._counts_[key][i]
                        
                data_index = data_index + 1
            
        labels = labels + '!'
        
        """ Use the default empty data if there is none """
        if not data:
            return {'num_elems' : 1, 'counts' : [len(empty_data)], 'labels' : '!no_data?!', 'data' : empty_data, 'max_data' : 0.0, 'min_data' : 0.0}
        else:
            return {'num_elems' : data_index, 'counts' : counts, 'labels' : labels, 'data' : data, 'max_data' : max_data, 'min_data' : min_data}

"""

    CLASS :: IntervalReporting
    
    Performs queries that take timestamps, query, and an interval as arguments.  Data for a single metric 
    is generated for each time interval in the time period defined by the start and end timestamps. 
    
    
    Members:
    ========
    
    _filters_     - stores a dictionary { filter_name : [filter_object, ]}
    
"""

class IntervalReporting(DataReporting):
    
    """
        Constructor for IntervalReporting
        
        INPUT:
        
            loader_type    - string which determines the type of dataloader object
            **kwargs       - allows plotting parameters to be tuned     
    """
    def __init__(self, **kwargs):
        
        self._data_loader_ = DL.IntervalReportingLoader(query_type='')
        
        for key in kwargs:
            if key == 'query_type':                          # Set custom data loaders
                if kwargs[key] == 'campaign':
                    self._data_loader_ = DL.CampaignIntervalReportingLoader(**kwargs)
                else:
                    self._data_loader_ = DL.IntervalReportingLoader(**kwargs)
            
            elif key == 'was_run':
                self._was_run_ = kwargs[key]
            
                
        """ Call constructor of parent """
        DataReporting.__init__(self, **kwargs)


    """
        Selecting a subset of the key items in a dictionary       
        
        INPUT:
            dict_lists    - dictionary to be parsed
                        
        RETURN:
            new_dict_lists    - new dictionary containing only keys in self._item_keys_
    """
    def select_metric_keys(self, dict_lists):
        new_dict_lists = dict()
        
        dict_lists_keys = dict_lists.keys()
        
        for key in self._item_keys_:
            if key in dict_lists_keys:
                new_dict_lists[key] = dict_lists[key]
        
        return new_dict_lists
        
    """
        Protected method.  Execute reporting query and generate plots.       
        
        INPUT:    The inputs define the plot arguments                
        
    """        
    def _gen_plot(self, metrics, times, title, xlabel, ylabel, ranges, subplot_index, fname, labels):
        
        pylab.subplot(subplot_index)
        pylab.figure(num=None,figsize=[26,14])    
        
        #line_types = ['b-o','g-o','r-o','c-o','m-o','k-o','y-o','b--d','g--d','r--d','c--d','m--d','k--d','y--d','b-.s','g-.s','r-.s','c-.s','m-.s','k-.s','y-.s']
        line_types = ['b-o','g-x','r-s','c-d','m-o','k-o','y-o','b--d','g--d','r--d','c--d','m--d','k--d','y--d','b-.s','g-.s','r-.s','c-.s','m-.s','k-.s','y-.s']

        count = 0
        for key in metrics.keys():
            if self._plot_type_ == 'step':
                pylab.step(times[key], metrics[key], line_types[count], linewidth=3.0)
            elif self._plot_type_ == 'line':
                pylab.plot(times[key][1:], metrics[key][1:], line_types[count], linewidth=3.0)
            count = count + 1
        
        """ Set the figure and font size """
        
        golden_mean = (math.sqrt(5)-1.0)/2.0                    # Aesthetic ratio
        fig_width = self._fig_width_pt_*self._inches_per_pt_    # width in inches
        fig_height = fig_width*golden_mean                      # height in inches
        fig_size =  [fig_width,fig_height]

        params = {'axes.labelsize': self._font_size_,
          'text.fontsize': self._font_size_,
          'xtick.labelsize': self._font_size_,
          'ytick.labelsize': self._font_size_,
          'legend.pad': 0.1,     # empty space around the legend box
          'legend.fontsize': self._font_size_,
          'font.size': self._font_size_,
          'text.usetex': False,
          'figure.figsize': fig_size}

        pylab.rcParams.update(params)

        pylab.grid()
        pylab.xlim(ranges[0], ranges[1])
        pylab.ylim(ranges[2], ranges[3])
        
        if self._use_labels_:
            pylab.legend(labels,loc=2)
        else:
            pylab.legend(metrics.keys(),loc=2)
        

        pylab.xlabel(xlabel)
        pylab.ylabel(ylabel)

        pylab.title(title)
        
        _data_plot_ = pylab.savefig(self._file_path_ + fname + '.' + self._fig_file_format_, format=self._fig_file_format_)



    """
        Use dataloader to produce object state - counts and times.  
        
        The general flow comprises of:
        
        <generate state> -> <post processing of state data> -> <generate plot>
        
        INPUT:
                The inputs serve as query arguments to generate the data
         
    """        
    def run(self, start_time, end_time, interval, metric_name, campaign, label_dict, **kwargs):
        
        """ 
            PROCESS KWARGS
            ==============
        """
    
        if 'generate_plot' in kwargs:
            self._generate_plot_ = kwargs['generate_plot']

            
        if 'include_all_artifacts' in kwargs:
            artifact_keys_var = self._counts_.keys()
            for key in artifact_keys_var:
                label_dict[key] = key
        
            
        """ Get the artifacts from label dictionary """
        artifact_keys_var = label_dict.keys()
        
        """ Execute the query that generates interval reporting data """
        return_val = self._data_loader_.run_query(start_time, end_time, interval, metric_name, campaign, **kwargs)
        self._counts_ = return_val[0]
        self._times_ = return_val[1]
        
            
        """ Filter the data """        
        self.add_filters_runtime(interval=interval, artifact_keys=artifact_keys_var, time_series=True)            
        self._execute_filters()
        
        
        """ COMPOSE a plot of the data """
        if self._generate_plot_:
            
            xlabel = 'MINUTES'
            subplot_index = 111
            fname = campaign + '_' + self._data_loader_._query_type_ + '_' + metric_name
            
            metric_full_name = QD.get_metric_full_name(metric_name)
            title = campaign + ':  ' + metric_full_name + ' -- ' + TP.timestamp_convert_format(start_time,1,2) + ' - ' + TP.timestamp_convert_format(end_time,1,2)
            ylabel = metric_full_name
            
            """ Determine List Maxima and Minima -- Pre-processing for plotting """
            times_max = 0
            metrics_max = 0
            
            for key in self._counts_.keys():
                list_max = max(self._counts_[key])
                if list_max > metrics_max:
                    metrics_max = list_max 
            
            for key in self._times_.keys():
                list_max = max(self._times_[key])
                if list_max > times_max:
                    times_max = list_max
            
            """ Set ranges for plotting based on maxima/minima """ 
            ranges = list()
            ranges.append(0.0)
            ranges.append(times_max * 1.1)
            ranges.append(0.0)
            ranges.append(metrics_max * 1.5)
            
            """ Collect the labels into a list """
            if len(artifact_keys_var) > 0:
                labels = list()
                for key in self._counts_.keys():
                    labels.append(label_dict[key])
            else:
                labels = self._counts_.keys()
            
            """ Generate plots given data """
            try:
                self._gen_plot(self._counts_, self._times_, title, xlabel, ylabel, ranges, subplot_index, fname, labels)
            
            except Exception as inst:
                
                logging.error('IntervalReporting -- Could not generate plot object ')
                logging.error(type(inst))     # the exception instance
                logging.error(inst.args)      # arguments stored in .args
                logging.error(inst)           # __str__ allows args to printed directly            
                
                pass
    
            
    
"""

    CLASS :: ConfidenceReporting
    
    This class uses ConfidenceReportingLoader and HypothesisTest to execute a confidence analysis over a given campaign. 
    
"""
class ConfidenceReporting(DataReporting):
    
        
    """
    
        Constructor for confidence reporting class
        
        INPUT:
        
            hypothesis_test    - an instance reflecting the type of test being used
            
        
    """
    def __init__(self, **kwargs):
        
        if not('query_type' in kwargs.keys()):
            err_msg = 'ConfidenceReporting obj must be created with a query type.'
            logging.error(err_msg)
            raise self.KwargDefError(err_msg)
        else:
            self._data_loader_ = DL.IntervalReportingLoader(**kwargs)
            
        if not('hyp_test' in kwargs.keys()):
            err_msg = 'ConfidenceReporting obj must be created with a hypothesis test type'
            logging.error(err_msg)
            raise self.KwargDefError(err_msg)
        else:
            # "Use only a ttest for now
            self._hypothesis_test_ = TTest()
                                    
        DataReporting.__init__(self, **kwargs)
        
        
        # self._data_loader_ = DL.HypothesisTestLoader()
        
    """
        Describes how to run a confidence report
    """    
    def usage(self): 

        print 'Method Signature:'
        print "    run(self, test_name, query_name, metric_name, campaign, items, start_time, end_time, interval, num_samples)"
        print ''
        print 'e.g.'
        print "    cr.run('mytest!','report_banner_confidence','don_per_imp', 'C_JMvTXT_smallcountry_WS', ['banner1','banner2'], '20110101000000', '20100101000000', 2, 20)"
        print ''
        print "    Keyword arguments may also be specified to modify plots:"
        print ''
        print "    font_size            - font size related to plots"
        print "    fig_width_pt         - width of the plot"    
        print "    inches_per_pt        - define point size relative to screen"
        print "    use_labels           - whether to include specified labels in plot"
        print "    fig_file_format      - file format of the image (default = .png)"
        print "    hyp_test             - the type of hypothesis test" 
        
        return
    
    
    """
        Confidence plotting over test intervals.  This plot takes into consideration test intervals displaying the means with error bars over each interval
        
    """
    def _gen_plot(self,means_1, means_2, std_devs_1, std_devs_2, times_indices, title, xlabel, ylabel, ranges, subplot_index, labels, fname):
                
        pylab.subplot(subplot_index)
        pylab.figure(num=None,figsize=[26,14])    
        
        e1 = pylab.errorbar(times_indices, means_1, yerr=std_devs_1, fmt='xb-')
        e2 = pylab.errorbar(times_indices, means_2, yerr=std_devs_2, fmt='dr-')
        # pylab.hist(counts, times)
        
        """ Set the figure and font size """
        fig_width_pt = 246.0  # Get this from LaTeX using \showthe\columnwidth
        inches_per_pt = 1.0/72.27               # Convert pt to inch
        golden_mean = (math.sqrt(5)-1.0)/2.0         # Aesthetic ratio
        fig_width = fig_width_pt*inches_per_pt  # width in inches
        fig_height = fig_width*golden_mean      # height in inches
        fig_size =  [fig_width,fig_height]
        
        font_size = 20
        
        params = { 'axes.labelsize': font_size,
          'text.fontsize': font_size,
          'xtick.labelsize': font_size,
          'ytick.labelsize': font_size,
          'legend.pad': 0.1,     # empty space around the legend box
          'legend.fontsize': font_size,
          'font.size': font_size,
          'text.usetex': False,
          'figure.figsize': fig_size}
        
        pylab.rcParams.update(params)
        
        pylab.grid()
        pylab.ylim(ranges[2], ranges[3])
        pylab.xlim(ranges[0], ranges[1])
        pylab.legend([e1[0], e2[0]], labels,loc=2)
        
        pylab.xlabel(xlabel)
        pylab.ylabel(ylabel)
        
        pylab.title(title)
        pylab.savefig(self._file_path_ + fname + '.' + self._fig_file_format_, format=self._fig_file_format_)
    
    
    """
        Generates a box plot of all of the data.  Does not visualize test intervals.
    """
    def _gen_box_plot(self, data, title, ylabel, subplot_index, labels, fname):
                
        
        pylab.subplot(subplot_index)
        pylab.figure(num=None,figsize=[26,14])    
        
        pylab.boxplot(data, sym='b+')
        pylab.xticks(range(1, len(labels) + 1), labels)
        
        """ Set the figure and font size """
        fig_width_pt = 246.0  # Get this from LaTeX using \showthe\columnwidth
        inches_per_pt = 1.0/72.27               # Convert pt to inch
        golden_mean = (math.sqrt(5)-1.0)/2.0         # Aesthetic ratio
        fig_width = fig_width_pt*inches_per_pt  # width in inches
        fig_height = fig_width*golden_mean      # height in inches
        fig_size =  [fig_width,fig_height]
        
        font_size = 20
        
        params = { 'axes.labelsize': font_size,
          'text.fontsize': font_size,
          'xtick.labelsize': font_size,
          'ytick.labelsize': font_size,
          'legend.pad': 0.1,     # empty space around the legend box
          'legend.fontsize': font_size,
          'font.size': font_size,
          'text.usetex': False,
          'figure.figsize': fig_size}
        
        pylab.rcParams.update(params)
        
        pylab.grid()
        
        pylab.ylabel(ylabel)
        
        pylab.title(title)
        pylab.savefig(self._file_path_ + fname + '.' + self._fig_file_format_, format=self._fig_file_format_)
        
    
    """ 
        Executes the test reporting.
        
        @param num_samples: the number of samples per test interval
        @type num_samples: integer
        
        @param interval: the length of the test interval in minutes
        @type interval: integer
        
        @param items: datasets for paired testing
        @type items: dictionary
        
        
    """
    def run(self, test_name, campaign, metric_name, items, start_time, end_time, interval, **kwargs):
        
    
        """ TEMPORARY - map TODO : this should be more generalized """
        counter = 1
        for key in items.keys():
            if counter == 1:
                label_1 = items[key]
                item_1 = key
            elif counter == 2:
                label_2 = items[key]
                item_2 = key
            counter += 1
        
        artifact_keys_var = items.keys()
        
        """ Retrieve values from database """
        results = self._data_loader_.run_query(start_time, end_time, interval, metric_name, campaign, **kwargs)
        self._counts_ = results[0]
        self._times_ = results[1]
        
        """ Filter the data """
        self.add_filters_runtime(interval=interval, artifact_keys=artifact_keys_var, time_series=True)
        self._execute_filters()
        
        metrics = self._counts_
        times_indices = self._times_
        
        try:
            metrics_1 = metrics[item_1]
            metrics_2 = metrics[item_2]
            
        except:
            
            logging.error('No data for confidence reporting.  Missing key.')
            return ['-- ', '0.00', 'Inconclusive']
        
        """ run the confidence test """
        num_samples = len(metrics_1)
        ret = self._hypothesis_test_.confidence_test(metrics_1, metrics_2, num_samples)
        means_1 = ret[0]
        means_2 = ret[1]
        std_devs_1 = ret[2]
        std_devs_2 = ret[3]
        confidence = ret[4]
        colour_code = ret[5] 

        """ plot the results """
        # xlabel = 'Hours'
        subplot_index = 111
        fname = campaign + '_conf_' + metric_name
        
        title = confidence + '\n\n' + test_name + ' -- ' + TP.timestamp_convert_format(start_time,1,2) + ' - ' + TP.timestamp_convert_format(end_time,1,2)
        
        max_mean = max(max(means_1),max(means_2))
        max_sd = max(max(std_devs_1),max(std_devs_2))
        max_y = float(max_mean) + float(max_sd) 
        max_y = max_y + 0.5 * max_y
        # max_x = max(times_indices) + min(times_indices)
        # ranges = [0.0, max_x, 0, max_y]
        
        ylabel = QD.get_metric_full_name(metric_name)
        labels = [label_1, label_2]
        
        try:
            # self._gen_plot(means_1, means_2, std_devs_1, std_devs_2, times_indices, title, xlabel, ylabel, ranges, subplot_index, labels, fname)
            self._gen_box_plot([metrics_1, metrics_2], title, ylabel, subplot_index, labels, fname)
        
        except Exception as inst:
            
            logging.error('ConfidenceReporting -- Could not generate plot object ')
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)      # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly            
            
            pass
        
        return [confidence, colour_code]


    """ 
        Build a legend table for confidence reporting
        
        @return: formatted table
    """ 
    def get_confidence_legend_table(self):
        
        logging.info('Generating confidence colour legend.')
        
        column_names = ['<b>Confidence Level</b>']
        rows = [['<td><b>Colour Code</b></td>']]                
        
        max_index = len(TTest._probs_) 
        for index in range(max_index):
            
            if index == 0:
                conf_range = '0%s - %5.2f%s' % ('%', (1.0 - TTest._probs_[index]) * 100.0, '%')
            else:
                conf_range = '%5.2f%s - %5.2f%s' % ((1.0 - TTest._probs_[index-1]) * 100.0, '%', (1.0 - TTest._probs_[index]) * 100.0, '%')
            
            column_names.append(conf_range)
            
            # intensity = float(index + 1) / float(max_index)  
            # colour_index = TTest().get_confidence_colour(intensity)
            colour_index = TTest().get_confidence_colour_by_index(index)
            cell = '<td style="background-color:' + colour_index + ';"></td>'
            
            rows[0].append(cell)
        
        column_names.append('Winner')
        winning_colour_index = TTest().get_confidence_winner_colour()
        rows[0].append('<td style="background-color:' + winning_colour_index + ';"></td>')
        
        return self._write_html_table(rows, column_names, omit_cell_markup=True) 
    
    """
        Get minutely data and compute confidence 
        
        @param start_time, end_time: timestamp objects
        
        @return - colour indices of confidence on values
        
    """
    def get_confidence_on_time_range(self, start_time, end_time, campaign, **kwargs):
        
        sampling_interval = 1
        if 'sampling_interval' in kwargs:
            if isinstance(kwargs['sampling_interval'], int):
                sampling_interval = kwargs['sampling_interval']        

        
        derived_metrics = ['donations', 'click_rate', 'don_per_imp', 'amt_norm_per_imp', 'don_per_view', 'amt_norm_per_view']
        measured_metrics = ['impressions', 'views', 'donations', 'amount_normal']
        measured_metrics_counts = dict()
        derived_metrics_counts = dict()
        conf_colour_code = dict()
        
        ir = IntervalReporting(query_type='bannerlp')
        t_test = TTest()
         
        logging.info('Getting minutely live data for hypothesis testing ...')
        
        if 'measured_metrics_counts' in kwargs:
            measured_metrics_counts = kwargs['measured_metrics_counts']
            
        else:
            
            for metric in measured_metrics:                                
                ir.run(start_time, end_time, sampling_interval, metric, campaign, {}, include_all_artifacts=True, generate_plot=False, **kwargs)
                measured_metrics_counts[metric] = ir._counts_
        
        """ Generate the Table only if there is data available  """
        try:
            
            metric = measured_metrics_counts.keys()[0]
            
            artifact_key_list = measured_metrics_counts[metric].keys()
            first_key = artifact_key_list[0]
            num_samples_base = len(measured_metrics_counts[metric][first_key])
            
            """ 
                Convert to measured metrics 
                
                Since many of the computed metrics in the SQL queries are subject to noise raw values are used to recompute only valid samples
                Then the 
            """
            min_impression_threshold = 500
            min_view_threshold = 10
            sample_threshold = 10
            
            for metric in derived_metrics:
                
                conf_colour_code[metric] = dict()
                derived_metrics_counts[metric] = dict()
                
                """ Initialize sample list """
                for artifact_key in artifact_key_list:
                    derived_metrics_counts[metric][artifact_key] = list()
                
                min_sample_count = 100000   # this is used to determine the artifact with the lowest number of samples
                keys_to_remove = list()
                for artifact_key in artifact_key_list:
                    
                    """
                        Cycle through the samples for each artifact for the given metric.  Determine whether to include the sample if it satisfies the minimum number of impressions.  
                        This is used to filter noisy samples from the results.  Note that filters are relative to the maximum values however there is a lower absolute filter
                        
                    """
                    for index in range(num_samples_base):
                        if metric == 'donations':
                            derived_metrics_counts[metric][artifact_key].append(measured_metrics_counts['donations'][artifact_key][index])
                        if metric == 'click_rate':
                            impression_threshold = max([float(max(measured_metrics_counts['impressions'][artifact_key])) * 0.50, min_impression_threshold])
                            if measured_metrics_counts['impressions'][artifact_key][index] > impression_threshold:
                                derived_metrics_counts[metric][artifact_key].append(measured_metrics_counts['views'][artifact_key][index] / measured_metrics_counts['impressions'][artifact_key][index])
                        if metric == 'don_per_imp':
                            impression_threshold = max([float(max(measured_metrics_counts['impressions'][artifact_key])) * 0.50, min_impression_threshold])
                            if measured_metrics_counts['impressions'][artifact_key][index] > impression_threshold:
                                derived_metrics_counts[metric][artifact_key].append(measured_metrics_counts['donations'][artifact_key][index] / measured_metrics_counts['impressions'][artifact_key][index])
                        elif metric == 'don_per_view':
                            view_threshold = max([float(max(measured_metrics_counts['views'][artifact_key])) * 0.50, min_view_threshold])
                            if measured_metrics_counts['views'][artifact_key][index] > view_threshold:
                                derived_metrics_counts[metric][artifact_key].append(measured_metrics_counts['donations'][artifact_key][index] / measured_metrics_counts['views'][artifact_key][index])
                        elif metric == 'amt_norm_per_imp':
                            impression_threshold = max([float(max(measured_metrics_counts['impressions'][artifact_key])) * 0.50, min_impression_threshold])
                            if measured_metrics_counts['impressions'][artifact_key][index] > impression_threshold:
                                derived_metrics_counts[metric][artifact_key].append(measured_metrics_counts['amount_normal'][artifact_key][index] / measured_metrics_counts['impressions'][artifact_key][index])
                        elif metric == 'amt_norm_per_view':
                            view_threshold = max([float(max(measured_metrics_counts['views'][artifact_key])) * 0.50, min_view_threshold])
                            if measured_metrics_counts['views'][artifact_key][index] > view_threshold:
                                derived_metrics_counts[metric][artifact_key].append(measured_metrics_counts['amount_normal'][artifact_key][index] / measured_metrics_counts['views'][artifact_key][index])
            
                    if len(derived_metrics_counts[metric][artifact_key]) < sample_threshold:
                        keys_to_remove.append(artifact_key)
                    elif len(derived_metrics_counts[metric][artifact_key]) < min_sample_count:
                        min_sample_count = len(derived_metrics_counts[metric][artifact_key])
                
                logging.info('%s samples used in hypothesis testing for metric %s.' % (str(min_sample_count), metric))
                
                for artifact_key in keys_to_remove:
                    del derived_metrics_counts[metric][artifact_key]
                
                for artifact_key in derived_metrics_counts[metric]:
                    
                    """ 
                        If the number of samples for a given artifact and metric exceed the minimum then fold the extra samples into the  
                        
                        The folding is accomplished by averaging in the extra samples 
                        
                    """
                    num_samples_curr = len(derived_metrics_counts[metric][artifact_key])
                    if num_samples_curr > min_sample_count:
                        # derived_metrics_counts[metric][artifact_key] = derived_metrics_counts[metric][artifact_key][:min_sample_count]
                        
                        weight = 1.0
                        for index in range(min_sample_count, num_samples_curr):
                            
                            relative_index = index % min_sample_count
                            
                            if relative_index == 0:
                                weight = weight + 1.0
                                
                            derived_metrics_counts[metric][artifact_key][relative_index] = derived_metrics_counts[metric][artifact_key][relative_index] * ((weight - 1) / weight) + \
                            derived_metrics_counts[metric][artifact_key][index] * (1 / weight)
                        
                        derived_metrics_counts[metric][artifact_key] = derived_metrics_counts[metric][artifact_key][:min_sample_count]
                
        
        
                """ Find the winner for a given metric """
                metric_max = 0.0
                for artifact_key in derived_metrics_counts[metric]:
                    if sum(derived_metrics_counts[metric][artifact_key]) > metric_max:
                        winner = artifact_key
                        metric_max = sum(derived_metrics_counts[metric][artifact_key])
                
                conf_colour_code[metric][winner] = TTest().get_confidence_winner_colour() # the winner cell is coloured accordinglt
                for  artifact_key in derived_metrics_counts[metric]:
                    
                    if artifact_key != winner:
                        num_samples = len(derived_metrics_counts[metric][winner])
                        ret = t_test.confidence_test(derived_metrics_counts[metric][winner], derived_metrics_counts[metric][artifact_key], num_samples)
                        
                        """ ret[5] == colour_index - Get the colour coding for the winner """
                        conf_colour_code[metric][artifact_key] = ret[5]
        
        except:
            
            conf_colour_code = {}
            measured_metrics_counts = {}
                                            
        return conf_colour_code, measured_metrics_counts

"""
"""
class DonorBracketReporting(DataReporting):
        
    """    Constructor
        
            @param **kwargs: the query type (string) for the dataloader
                             the (boolean) value indicating whether the query for this object has been executed

    """
    def __init__(self, **kwargs):
        
        query_type = FDH._QTYPE_BANNER_
        
        for key in kwargs:
            if key == 'query_type':                          # Set custom data loaders
                query_type = kwargs[key]
                        
            elif key == 'was_run':
                self._was_run_ = kwargs[key]
            
        self._data_loader_ = DL.DonorBracketsReportingLoader(query_type=query_type)
            
        """ Call constructor of parent """
        DataReporting.__init__(self, **kwargs)
    
    """ Protected method.  Generate plots.       
        
        @param bracket_names: the categories that the 
        
    """        
    def _gen_plot(self, bracket_names, data, metric_name, title, fname):

        spacing = 0.1
        offset_spacing = 0
        total_width = 0.3
        width = total_width / (3 * float(len(data.keys())))             
        
        """ Generate a histogram for each artifact """
        subplot_index = 111

        colours = ['r', 'b', 'y', 'g', 'c', 'm', 'k']
        iter_colours = iter(colours)
        indices = range(len(bracket_names))
        
        rects = list()
        
        """ Build the tick labels for the  """
        tick_pos = list()
        for i in indices:            
            tick_pos.append(spacing + width + i * spacing + i * width)
            
        plt.clf()
        plt.subplot(subplot_index)
        plt.figure(num=None,figsize=[26,14])
        plt.xticks(tick_pos, bracket_names)
        plt.grid()
        plt.title(title)
        plt.ylabel(metric_name)
        
        for artifact in data:  
            
            """ Position the bars and the bar labels """
            bar_pos = list()
            
            for i in indices:
                bar_pos.append(spacing + i * spacing + i * width + offset_spacing)
            
            """ plot the donations """    
            rects.append(plt.bar(bar_pos, data[artifact], width, color=iter_colours.next())[0])
            
            """ increment the offset spacing for bars"""
            offset_spacing = offset_spacing + width
        
        plt.legend(rects, data.keys())
        plt.savefig(self._file_path_ + fname + '_' + metric_name + '.' + self._fig_file_format_, format=self._fig_file_format_)
        
    """ 
    """ 
    def run(self, start_time, end_time, campaign):
        
        """ retrieve rows from query on donor brackets """
        data = self._data_loader_.run_query(start_time, end_time, campaign)
        
        """ Compose a plot of the data """
        if self._generate_plot_:
            
            plot_title =  'Donor Dollar Breakdown:  ' + campaign + ' -- ' + TP.timestamp_convert_format(start_time,1,2) + ' - ' + TP.timestamp_convert_format(end_time,1,2)
            fname = 'donor_brackets_' + campaign # + '_' + self._data_loader_._query_type_
                        
            """ Generate plots given data """
            bracket_names = data[0]
            bracket_names = bracket_names[bracket_names.keys()[0]]
            
            try:
                self._gen_plot(bracket_names, data[1], 'donations', plot_title, fname)
                self._gen_plot(bracket_names, data[2], 'amounts', plot_title, fname)
            
            except Exception as inst:
            
                logging.error('DonorBracketReporting -- Could not generate plot object ')
                logging.error(type(inst))     # the exception instance
                logging.error(inst.args)      # arguments stored in .args
                logging.error(inst)           # __str__ allows args to printed directly            
                
                pass



class CategoryReporting(DataReporting):
    
    """    Constructor
        
            @param **kwargs: allows plotting parameters to be tuned
    """
    def __init__(self, **kwargs):
        
        """ Process kwargs """
        for key in kwargs:
            if key == 'was_run':
                self._was_run_ = kwargs[key]
                
        """ Initialize data loader objects """
        self._PC_table_loader_ = DL.PageCategoryTableLoader()
        self._LP_table_loader_ = DL.LandingPageTableLoader()
        
        """ Call constructor of parent """
        DataReporting.__init__(self, **kwargs)
        
        return
    
    """
        Generates a bar plot of categories from banners
        
        @param category_counts: dictionary of integer counts keyed on category names
    """
    def _gen_plot_bar(self, category_counts, title, fname):
        
        """ Add category data to a list from dict object """
        data = list()
        for key in category_counts:
            data.append(category_counts[key])
        category_names = category_counts.keys()
        
        spacing = 0.5
        width = 0.3
        
        """ Generate a histogram for each artifact """
        subplot_index = 111

        colours = ['r', 'b', 'y', 'g']
        iter_colours = iter(colours)
        indices = range(len(category_names))
        
        rects = list()
        
        """ Build the tick labels for the  """
        tick_pos = list()
        for i in indices:            
            tick_pos.append(spacing + width + i * spacing + i * width)
            
        plt.clf()
        
        self._font_size_ = 14
        params = {'axes.labelsize': self._font_size_,
          'text.fontsize': self._font_size_,
          'xtick.labelsize': self._font_size_,
          'ytick.labelsize': self._font_size_,
          'legend.pad': 0.1,     # empty space around the legend box
          'legend.fontsize': self._font_size_,
          'font.size': self._font_size_,
          'text.usetex': False,
          'figure.figsize': [26,14]}

        plt.rcParams.update(params)
        
        plt.subplot(subplot_index)
        plt.figure(num=None,figsize=[26,14])
        plt.xticks(tick_pos, category_names)
        plt.grid()
        plt.title(title)
        plt.ylabel('% CHANGE')
        
        bar_pos = list()
        for i in indices:
            bar_pos.append(spacing + i * spacing + i * width + width / 2)
        rects.append(plt.bar(bar_pos, data, width, color=iter_colours.next())[0])
        
        # plt.legend(rects, data.keys())
        plt.savefig(self._file_path_ + fname + '_bar.' + self._fig_file_format_, format=self._fig_file_format_)
    
    """
        Generates a pie chart of categories from banners
        
        @param category_counts: dictionary of integer counts keyed on category names
    """
    def _gen_plot_pie(self, category_counts, title, fname):

        """ Add category data to a list from dict object """
        data = list()
        category_names = list()
        for key in category_counts:
            data.append(category_counts[key])
            category_names.append(key)

        plt.clf()

        params = {'axes.labelsize': self._font_size_,
          'text.fontsize': self._font_size_,
          'xtick.labelsize': self._font_size_,
          'ytick.labelsize': self._font_size_,
          'legend.pad': 0.1,     # empty space around the legend box
          'legend.fontsize': self._font_size_,
          'font.size': self._font_size_,
          'text.usetex': False,
          'figure.figsize': [26,14]}

        plt.rcParams.update(params)
        
        plt.subplot(111)
        plt.figure(num=None,figsize=[26,14])
        plt.grid()
        plt.title(title)
        plt.pie(data, labels=category_names)
        
        plt.savefig(self._file_path_ + fname + '_pie.' + self._fig_file_format_, format=self._fig_file_format_)
        

    """
        Execution method for category reporting.
        
        @param shortest_paths: List containing
    """    
    def run(self, start_time, end_time, campaign):
        
        timestamps = self._LP_table_loader_.get_log_start_times(start_time, end_time)
        
        start_time_formatted = TP.timestamp_convert_format(start_time, 1, 2) 
        end_time_formatted = TP.timestamp_convert_format(end_time, 1, 2)

        logging.info('Getting referred pages between %s and %s ...' % (start_time_formatted, end_time_formatted))
        page_ids = list()
        for ts in timestamps:                        
            page_ids.extend(self._LP_table_loader_.get_lp_referrers_by_log_start_timestamp(TP.timestamp_from_obj(ts,1,3), campaign)[0])
            
        logging.info('%s Referred pages ...' % str(len(page_ids)))
        # category_counts = self._PC_table_loader_.get_article_vector_counts(page_ids)
        category_counts = self._PC_table_loader_.get_normalized_category_counts(page_ids)            
        
        title = 'Histogram of Top Level Categories: %s - %s ' % (start_time_formatted, end_time_formatted)
        fname = 'referrer_categories_' + campaign
        
        self._gen_plot_bar(category_counts, title, fname)
        
        return category_counts
    
    