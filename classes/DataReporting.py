

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
import sys, pylab, math, logging, matplotlib.pyplot as plt


""" Import Analytics modules """
import Fundraiser_Tools.settings as projSet
import Fundraiser_Tools.classes.QueryData as QD
import Fundraiser_Tools.classes.Helper as Hlp
import Fundraiser_Tools.classes.TimestampProcessor as TP
import Fundraiser_Tools.classes.DataLoader as DL
import Fundraiser_Tools.classes.HypothesisTest as HT
import Fundraiser_Tools.classes.FundraiserDataHandler as FDH
import Fundraiser_Tools.classes.DataFilters as DF
import Fundraiser_Tools.classes.DataFilters as DF
import WSOR.scripts.classes.WSORSlaveDataLoader as WSDL


""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

        
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
    
    
    _font_size_ = 24
    _fig_width_pt_ = 246.0                  # Get this from LaTeX using \showthe\columnwidth
    _inches_per_pt_ = 1.0/72.27             # Convert pt to inch
    

    def __init__(self, **kwargs):
        
        """ Configure tunable reporting params  """
        self._use_labels_= False
        self._fig_file_format_ = 'png'
        self._plot_type_ = 'line'
        self._item_keys_ = list()
        self._file_path_ = './'
        self._generate_plot_ = True


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
    
        workaround for issue with tuple objects in HTML.py 
        MySQLdb returns unfamiliar tuple elements from its fetchall() method
        this is probably a version problem since the issue popped up in 2.5 but not 2.6
        
        INPUT:
                row               - row object returned from MySQLdb.fetchall()
                
        RETURN: 
                l                - a list of tuple objects from the db
    
    """
    def listify(self, row):
        l = []
        for i in row:
            l.append(i)
        return l
        
        
    
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
    
        To be overloaded by subclasses for writing tables - this functionality currently exists outside of this class structure (test_reporting.py)
        
        INPUT:
                values               - a list of datetime objects
                window_length       - indicate whether the list counts back from the end
            
        RETURN: 
                return_status        - integer, 0 indicates un-exceptional execution
    
    """
    def _write_html_table(self):
        return 0
                
    
    
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
        
        self._data_loader_ = DL.IntervalReportingLoader('')
        
        for key in kwargs:
            if key == 'query_type':                          # Set custom data loaders
                if kwargs[key] == 'campaign':
                    self._data_loader_ = DL.CampaignIntervalReportingLoader()
                else:
                    self._data_loader_ = DL.IntervalReportingLoader(kwargs[key])
            
            elif key == 'was_run':
                self._was_run_ = kwargs[key]
            
                
        """ Call constructor of parent """
        DataReporting.__init__(self, **kwargs)
    
        
    """
        Usage instructions for executing a report via the IntervalReporting class
    """    
    def usage(self): 
                
        print 'Types of queries:'
        print '    (1) banner'
        print '    (2) LP'
        print ''
        print 'e.g.'
        print "    run('20101230160400', '20101230165400', 2, 'banner', 'imp', '20101230JA091_US', ['banner1', 'banner2'])"
        print "    run('20101230160400', '20101230165400', 2, 'LP', 'views', '20101230JA091_US', [])"
        print ''
        print "    Keyword arguments may also be passed to the constructor:"
        print ''
        print "    font_size            - font size related to plots"
        print "    fig_width_pt         - width of the plot"    
        print "    inches_per_pt        - define point size relative to screen"
        print "    use_labels           - whether to include specified labels in plot"
        print "    fig_file_format      - file format of the image (default = .png)"
        print "    plot_type            - line or step plot"
        print "    item_keys            - the item keys expected (only these will be included)"
        print "    file_path            - override the file path to store the plot output" 
        print "    query_type           - the type of query to use"
    
        return
    

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
    
        Protected method.  Turns a dictionary into an html table.
        
        @param label_dict:
        @param top_metric: 
        @param links: 
        
        RETURN:
            
            html    - html text for the resulting table
    """ 
    def _write_html_table(self, data, highlighted_col_index):
        
        """ EXTRACT COLUMN NAMES AND ORDER THEM """
        index = data.keys()[0]
        col_names = data[index].keys()

        col_names = FDH.order_column_keys(col_names)
        
        html = '<table border=\"1\" cellpadding=\"10\"><tr>'
        
        
        """ Build headers """
        html = html + '<th>' + self._data_loader_._query_type_ + '</th>'
        for name in col_names:
            if name in highlighted_col_index:
                html = html + '<th style="background-color:orange;">' + name + '</th>'
            else:
                html = html + '<th>' + name + '</th>'
        html = html + '</tr>'
        
        """ Build rows """
        for item in data.keys():
            html = html + '<tr>'
            
            html = html + '<td>' + item + '</td>'
            
            for elem in col_names:                
                elem_formatted = QD.get_metric_data_type(elem, data[item][elem])
                
                if elem in highlighted_col_index:
                    html = html + '<td style="background-color:yellow;">' + elem_formatted + '</td>'
                else:
                    html = html + '<td>' + elem_formatted + '</td>'
            html = html + '</tr>'
        
        html = html + '</table>'        
        
        return html


    """
        Use dataloader to produce object state - counts and times.  
        
        The general flow comprises of:
        
        <generate state> -> <post processing of state data> -> <generate plot>
        
        INPUT:
                The inputs serve as query arguments to generate the data
         
    """        
    def run(self, start_time, end_time, interval, metric_name, campaign, label_dict):
        
        """ Get the artifacts from label dictionary """
        artifact_keys = label_dict.keys()
        
        """ Execute the query that generates interval reporting data """
        return_val = self._data_loader_.run_query(start_time, end_time, interval, metric_name, campaign)
        self._counts_ = return_val[0]
        self._times_ = return_val[1]
        
        """ TODO / FIX ME -- EVERYTHING AFTER THIS POINT SHOULD BE A FILTER """
        
        """ Select only the specified item keys """
        if len(self._item_keys_) > 0:
            self._counts_ = self.select_metric_keys(self._counts_)
            self._times_ = self.select_metric_keys(self._times_)

        """ Convert Times to Integers that indicate relative times AND normalize the intervals in case any are missing """
        for key in self._times_.keys():
            self._times_[key] = TP.normalize_timestamps(self._times_[key], False, 3)            
            self._times_[key], self._counts_[key] = TP.normalize_intervals(self._times_[key], self._counts_[key], interval)
        
        """ If there are missing metrics add them as zeros """
        for artifact_key in artifact_keys:

            if not(artifact_key in self._times_.keys()):
                self._times_[artifact_key] = self._times_[self._times_.keys()[0]]
                self._counts_[artifact_key] = [0.0] * len(self._times_[artifact_key])
        
        """  Remove artifacts not in the list if there are any labels specified """
        if len(artifact_keys) > 0:
            for key in self._counts_.keys():
                if key not in artifact_keys:
                    del self._counts_[key]
                    del self._times_[key]
        
        """ Filter the data """
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
            if len(artifact_keys) > 0:
                labels = list()
                for key in self._counts_.keys():
                    labels.append(label_dict[key])
            else:
                labels = self._counts_.keys()
            
            """ Generate plots given data """
            self._gen_plot(self._counts_, self._times_, title, xlabel, ylabel, ranges, subplot_index, fname, labels)
            
        """ Generate table html """
        # self._write_html_table()

    
    
    
"""

    CLASS :: ConfidenceReporting
    
    This class uses ConfidenceReportingLoader and HypothesisTest to execute a confidence analysis over a given campaign. 
    
"""
class ConfidenceReporting(DataReporting):
    
    _hypothesis_test_ = None
    
    
    """
    
        Constructor for confidence reporting class
        
        INPUT:
        
            hypothesis_test    - an instance reflecting the type of test being used
            
        
    """
    def __init__(self, **kwargs):
        
        
        for key in kwargs:
            
            if key == 'hyp_test':                          # Set the hypothesis test
                if kwargs[key] == 't_test':
                    self._hypothesis_test_ = HT.TTest()
        
        # print self._hypothesis_test_.__str__
        
        self._data_loader_ = DL.HypothesisTestLoader()
        DataReporting.__init__(self, **kwargs)
        
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
        Print in Tabular form the means and standard deviation of each group over each interval.  Provides a detailed numerical output 
        of the analysis.
        
        RETURN: the winner string, percent increase of the winner for the metric
        
    """
    def print_metrics(self, filename, metric_name, means_1, means_2, std_devs_1, std_devs_2, times_indices, labels, test_call):
        
        filename += '.txt'
        file = open(self._file_path_ + filename, 'w')
        
        """ Compute % increase and report """
        try:
            av_means_1 = sum(means_1) / len(means_1)
            av_means_2 = sum(means_2) / len(means_2)
            percent_increase = math.fabs(av_means_1 - av_means_2) / min(av_means_1,av_means_2) * 100.0
        
        except Exception as inst:
            
            print 'Percent increase could not be computed.'
            print type(inst)     # the exception instance
            print inst.args      # arguments stored in .args
            print inst           # __str__ allows args to printed directly
            
            percent_increase = 0.0
            
        """ Compute the average standard deviations """
        av_std_dev_1 = 0
        av_std_dev_2 = 0
        
        for i in range(len(std_devs_1)):
            av_std_dev_1 = av_std_dev_1 + math.pow(std_devs_1[i], 2)
            av_std_dev_2 = av_std_dev_2 + math.pow(std_devs_2[i], 2)
        
        av_std_dev_1 = math.pow(av_std_dev_1 / len(std_devs_1), 0.5) 
        av_std_dev_2 = math.pow(av_std_dev_2 / len(std_devs_1), 0.5) 
        
        """ Assign the winner """    
        if av_means_1 > av_means_2:
            winner = labels[0]
        else:
            winner = labels[1]
        
        win_str =  '\nThe winner "' + winner + '" had a %.2f%s increase.'
        win_str = win_str % (percent_increase, '%')
        
        file.write('\nCOMMAND = ' + test_call)
                 

        file.write('\n\n' +  metric_name)
        file.write('\nitem 1  = ' + labels[0] + '\n')
        file.write('\nitem 2  = ' + labels[1] + '\n')
        file.write(win_str)
        file.write('\n\ninterval\tmean1\t\tmean2\t\tstddev1\t\tstddev2\n\n')
        
        
        """ Print out the parameters for each interval """
        
        for i in range(len(times_indices)):
            line_args = str(i) + '\t\t' + '%.5f\t\t' + '%.5f\t\t' + '%.5f\t\t' + '%.5f\n'
            line_str = line_args % (means_1[i], means_2[i], std_devs_1[i], std_devs_2[i])
#            print  line_str
            file.write(line_str)
        
        """ Print out the averaged parameters """
        line_args = '%.5f\t\t' + '%.5f\t\t' + '%.5f\t\t' + '%.5f\n'
        line_str = line_args % (av_means_1, av_means_2, av_std_dev_1, av_std_dev_2)

        
        file.write('\n\nOverall Parameters:\n')
        file.write('\nmean1\t\tmean2\t\tstddev1\t\tstddev2\n')
        file.write(line_str)
                        
        file.close()
        
        return [winner, percent_increase]
    
    """ 
        Executes the test reporting.
        
        @param num_samples: the number of samples per test interval
        @type num_samples: integer
        
        @param interval: the length of the test interval in minutes
        @type interval: integer
        
        @param items: datasets for paired testing
        @type items: dictionary
        
        
    """
    def run(self, test_name, query_name, metric_name, campaign, items, start_time, end_time, interval, num_samples):
        
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
                
        """ Retrieve values from database """
        ret = self._data_loader_.run_query(query_name, metric_name, campaign, item_1, item_2, start_time, end_time, interval, num_samples)
        metrics_1 = Hlp.convert_Decimal_list_to_float(ret[0])
        metrics_2 = Hlp.convert_Decimal_list_to_float(ret[1])
        times_indices = ret[2]

        """ run the confidence test """
        ret = self._hypothesis_test_.confidence_test(metrics_1, metrics_2, num_samples)
        means_1 = ret[0]
        means_2 = ret[1]
        std_devs_1 = ret[2]
        std_devs_2 = ret[3]
        confidence = ret[4]
        
        logging.debug(ret)

        """ plot the results """
        xlabel = 'Hours'
        subplot_index = 111
        fname = campaign + '_conf_' + metric_name
        
        title = confidence + '\n\n' + test_name + ' -- ' + TP.timestamp_convert_format(start_time,1,2) + ' - ' + TP.timestamp_convert_format(end_time,1,2)
        
        max_mean = max(max(means_1),max(means_2))
        max_sd = max(max(std_devs_1),max(std_devs_2))
        max_y = float(max_mean) + float(max_sd) 
        max_y = max_y + 0.5 * max_y
        max_x = max(times_indices) + min(times_indices)
        ranges = [0.0, max_x, 0, max_y]
        
        ylabel = QD.get_metric_full_name(metric_name)
        labels = [label_1, label_2]
        
        # self._gen_plot(means_1, means_2, std_devs_1, std_devs_2, times_indices, title, xlabel, ylabel, ranges, subplot_index, labels, fname)
        self._gen_box_plot([metrics_1, metrics_2], title, ylabel, subplot_index, labels, fname)
        
        """ Print out results """ 
        test_call = "run('" + test_name + "', '" + query_name + "', '" + metric_name + "', '" + campaign + "', '" + \
            item_1 + "', '" + item_2 + "', '" + start_time + "', '" + end_time + "', " + str(interval) + ", " + str(num_samples) + ")"
        winner, percent_increase = self.print_metrics(fname, title, means_1, means_2, std_devs_1, std_devs_2, times_indices, labels, test_call)
    
        return [winner, percent_increase, confidence]



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
            
        self._data_loader_ = DL.DonorBracketsReportingLoader(query_type)
            
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

        colours = ['r', 'b', 'y', 'g']
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
            self._gen_plot(bracket_names, data[1], 'donations', plot_title, fname)
            self._gen_plot(bracket_names, data[2], 'amounts', plot_title, fname)
        


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
        
        spacing = 0.3
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
        plt.subplot(subplot_index)
        plt.figure(num=None,figsize=[26,14])
        plt.xticks(tick_pos, category_names)
        plt.grid()
        plt.title(title)
        plt.ylabel('Counts')
        
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
            page_ids.extend(self._LP_table_loader_.get_referrers(ts))
            
        # shortest_paths = self._category_loader_.unpickle_var('shortest_paths.p')
        logging.info('%s Referred pages ...' % str(len(page_ids)))
        # category_counts = self._PC_table_loader_.get_article_categories_by_page_ids(page_ids)
        category_counts = self._PC_table_loader_.get_article_vector_counts(page_ids)            
                    
        title = 'Histogram of Top Level Categories: %s - %s ' % (start_time_formatted, end_time_formatted)
        fname = 'referrer_categories_' + campaign
        
        self._gen_plot_bar(category_counts, title, fname)
        
        return category_counts
    
    