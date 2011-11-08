"""
    DJANGO VIEW DEFINITIONS:
    ========================
    
    Defines the views for Log Miner Logging (LML) application.  The LML is meant to provide functionality for observing log mining activity and to
    copy and mine logs at the users request.

    Views:
    
    index           -- the index page shows a listing of generated tests by looking at the test table in the FR database via a DataLoader  
    test            -- This view is responsible for generating the test results via DataReporting object  
    add_comment     -- Handles the comment form when users wish to add them 

    Helper:
    
    auto_gen        -- does the actual work of generating the test report plots and results
    
"""

__author__ = "Ryan Faulkner"
__revision__ = "$Revision$"
__date__ = "June 20th, 2011"


""" Import django modules """
from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect, HttpResponse
from django.core.urlresolvers import reverse

""" Import python base modules """
import sys, MySQLdb, logging, math, datetime

""" Import Analytics modules """
import classes.DataLoader as DL
import classes.DataReporting as DR
import classes.FundraiserDataHandler as FDH
import classes.TimestampProcessor as TP
import classes.QueryData as QD
import config.settings as projSet


""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

_beginning_time_ = '000001122334455'
_end_time_ = '99990011223344'

"""
    Index page for tests ... lists all existing tests and allows a new test to be run
"""
def index(request):
    
    
    
    """ 
        PROCESS POST DATA
        ================= 
    """
    
    err_msg = ''

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
            
    ttl = DL.TestTableLoader()
    test_rows = ttl.get_all_test_rows()
    
    """ Build a list of tests -- apply filters """
    l = []
    for i in test_rows:
        test_start_time = ttl.get_test_field(i, 'start_time')
        
        """ Ensure the timestamp is properly formatted """
        if TP.is_timestamp(test_start_time, 2):
            test_start_time = TP.timestamp_convert_format(test_start_time, 2, 1)
        
        if int(test_start_time) > int(earliest_utc_ts_var) and int(test_start_time) < int(latest_utc_ts_var):
            l.append(i)
        
    l.reverse()
    
    return render_to_response('tests/index.html', {'err_msg' : err_msg, 'test_rows' : l},  context_instance=RequestContext(request))


"""
    Produces a summary of all the tests that have been run
"""
def test_summaries(request):
    
    """ Initialize TestTableLoader """
    ttl = DL.TestTableLoader()
    test_rows = ttl.get_all_test_rows()
    
    """ Process test info / write html """
    html = ''
    for row in test_rows:
        
        test_name = ttl.get_test_field(row, 'test_name')
        test_type = ttl.get_test_field(row, 'test_type')
        utm_campaign = ttl.get_test_field(row, 'utm_campaign')
        start_time = ttl.get_test_field(row, 'start_time')
        end_time = ttl.get_test_field(row, 'end_time')
        
        try:
            test_type = FDH._TESTTYPE_VERBOSE_[test_type]
        except:
            test_type = 'unknown'
            pass
        
        try:
            summary_table = ttl.get_test_field(row, 'html_report').split('<!-- SUMMARY TABLE MARKER -->')[1]
            html = html + '<h1><u>' + test_name + ' -- ' +  utm_campaign + '</u></h1><div class="spacer"></div>' \
            + '<font size="4"><u>Test Type:</u>  ' + test_type  + '</font><div class="spacer_small"></div>' \
            + '<font size="4"><u>Running from</u> ' + start_time + ' <u>to</u> ' + end_time + '</font><div class="spacer"></div>' \
            + summary_table + '<div class="spacer"></div><div class="spacer"></div>' 
            
        except:
            pass
        
    return render_to_response('tests/test_summaries.html', {'template' : html},  context_instance=RequestContext(request))



"""
    Executes a test, builds a report, and adds it to the test table
    
"""
def test(request):
    
    """ 
        PROCESS POST DATA
        ================= 
        
        Escape all user input that can be entered in text fields 
        
    """
    test_name_var = MySQLdb._mysql.escape_string(request.POST['test_name'])
    utm_campaign_var = MySQLdb._mysql.escape_string(request.POST['utm_campaign'])
    start_time_var = MySQLdb._mysql.escape_string(request.POST['start_time'])
    end_time_var = MySQLdb._mysql.escape_string(request.POST['end_time'])    
    one_step_var = MySQLdb._mysql.escape_string(request.POST['one_step'])
    
    if cmp(one_step_var, 'True') == 0:
        one_step_var = True
    else:
        one_step_var = False
        
    try: 
        test_type_var = MySQLdb._mysql.escape_string(request.POST['test_type'])
        labels = request.POST['artifacts']
                        
    except KeyError:

        test_type_var, labels = FDH.get_test_type(utm_campaign_var, start_time_var, end_time_var, DL.CampaignReportingLoader(''))  # submit an empty query type           
        labels = labels.__str__() 
    
    label_dict = dict()
    label_dict_full = dict()
    
    labels = labels[1:-1].split(',')        
                                
    """ Parse the labels """     
    for i in range(len(labels)):
        labels[i] = labels[i]
        label = labels[i].split('\'')[1]
        label = label.strip()            
        pieces = label.split(' ')
        label = pieces[0]
        for j in range(len(pieces) - 1):
            label = label + '_' + pieces[j+1]
        
        """ Escape the label parameters """
        label = MySQLdb._mysql.escape_string(label)
        label_dict_full[label] = label
            
    """ Look at the artifact names and map them into a dict() - Determine if artifacts were chosen by the user """
    
    if request.POST.__contains__('artifacts_chosen'):
        
        artifacts_chosen =  request.POST.getlist('artifacts_chosen')
        
        """ Ensure that only two items are selected """
        if len(artifacts_chosen) > 2:
            raise Exception('Please select (checkboxes) exactly two items to test')
        
        for elem in artifacts_chosen:
            esc_elem = MySQLdb._mysql.escape_string(str(elem))
            label_dict[esc_elem] = esc_elem
    else:
        label_dict = label_dict_full

    
    """ Parse the added labels IF they are not empty """
    for key in label_dict.keys():
        
        try:
            if not(request.POST[key] == ''):
                
                label_dict[key] = MySQLdb._mysql.escape_string(str(request.POST[key]))
            else:
                label_dict[key] = key
        except:
            logging.error('Could not find %s in the POST QueryDict.' % key)
    
    for key in label_dict_full.keys():
        try:
            if not(request.POST[key] == ''):
                label_dict_full[key] = MySQLdb._mysql.escape_string(str(request.POST[key]))
            else:
                label_dict_full[key] = key
        except:
            logging.error('Could not find %s in the POST QueryDict.' % key)

    
    """ 
        EXECUTE REPORT GENERATION
        =========================
    
        setup time parameters
        determine test metrics
        execute queries
    """
    
    sample_interval = 1
    
    start_time_obj = TP.timestamp_to_obj(start_time_var, 1)
    end_time_obj = TP.timestamp_to_obj(end_time_var, 1)
    
    time_diff = end_time_obj - start_time_obj
    time_diff_min = time_diff.seconds / 60.0
    test_interval = int(math.floor(time_diff_min / sample_interval)) # 2 is the interval
        
    metric_types = FDH.get_test_type_metrics(test_type_var)
    metric_types_full = dict()
    
    
    """ Get the full (descriptive) version of the metric names 
        !! FIXME / TODO -- order these properly !! """
    
    for i in range(len(metric_types)):
        metric_types_full[metric_types[i]] = QD.get_metric_full_name(metric_types[i])
    
    
    """ USE generate_reporting_objects() TO GENERATE THE REPORT DATA - dependent on test type """
    
    measured_metric, winner, loser, percent_win, confidence, html_table_pm_banner, html_table_pm_lp, html_table_language, html_table \
    =  generate_reporting_objects(test_name_var, start_time_var, end_time_var, utm_campaign_var, label_dict, label_dict_full, \
                                  sample_interval, test_interval, test_type_var, metric_types, one_step_var)
    
    winner_var = winner[0]
    
    results = list()
    for index in range(len(winner)):
        results.append({'metric' : measured_metric[index], 'winner' : winner[index], 'loser': loser[index], 'percent_win' : percent_win[index], 'confidence' : confidence[index]})
        
    template_var_dict = {'results' : results,  \
              'utm_campaign' : utm_campaign_var, 'metric_names_full' : metric_types_full, \
              'summary_table': html_table, 'sample_interval' : sample_interval, \
              'banner_pm_table' : html_table_pm_banner, 'lp_pm_table' : html_table_pm_lp, 'html_table_language' : html_table_language}
    
    html = render_to_response('tests/results_' + test_type_var + '.html', template_var_dict, context_instance=RequestContext(request))
        

    
    """ 
        WRITE TO TEST TABLE
        =================== 
    
    """
    
    ttl = DL.TestTableLoader()
    
    """ Format the html string """
    html_string = html.__str__()
    html_string = html_string.replace('"', '\\"')

    if ttl.record_exists(utm_campaign=utm_campaign_var):
        ttl.update_test_row(test_name=test_name_var,test_type=test_type_var,utm_campaign=utm_campaign_var,start_time=start_time_var,end_time=end_time_var,html_report=html_string, winner=winner_var)
    else:
        ttl.insert_row(test_name=test_name_var,test_type=test_type_var,utm_campaign=utm_campaign_var,start_time=start_time_var,end_time=end_time_var,html_report=html_string, winner=winner_var)
    
    return html



"""

    Helper method for 'test' view which generates a report based on parameters
    

"""
def generate_reporting_objects(test_name, start_time, end_time, campaign, label_dict, label_dict_full, sample_interval, test_interval, test_type, metric_types, one_step_var):

    """ Labels will always be metric names in this case """
    # e.g. labels = {'Static banner':'20101227_JA061_US','Fading banner':'20101228_JAFader_US'}
    use_labels_var = True
    
    """ Build reporting objects """
    ir_cmpgn = DR.IntervalReporting(use_labels=False,font_size=20,plot_type='line',query_type='campaign',file_path=projSet.__web_home__ + 'campaigns/static/images/')
        
    """ 
        DETERMINE DONOR DOLLAR BREAKDOWN 
        ================================
    """
    try:
        DR.DonorBracketReporting(query_type=FDH._QTYPE_LP_, file_path=projSet.__web_home__ + 'tests/static/images/').run(start_time, end_time, campaign)
    except:
        pass
    
    """ 
        DETERMINE CATEGORY DISTRIBUTION 
        ===============================
    """
    if(0):
        DR.CategoryReporting(file_path=projSet.__web_home__ + 'tests/static/images/').run(start_time, end_time, campaign)
        
    
    """ 
        DETERMINE LANGUAGE BREAKDOWN 
        ============================
    """
    html_language = ''
    if(1):
        columns, data = DL.CiviCRMLoader().get_donor_by_language(campaign, start_time, end_time)
        html_language = DR.DataReporting()._write_html_table(data, columns)
        
    """ 
        DETERMINE PAYMENT METHODS 
        =========================
    """
    
    ccl = DL.CiviCRMLoader()
    pm_data_banner, pm_data_lp  = ccl.get_payment_methods(campaign, start_time, end_time)
    
    pm_data_banner_mapped = list()
    pm_data_lp_mapped = list()
    ccl.map_autoviv_to_list(pm_data_banner, pm_data_banner_mapped, [])
    ccl.map_autoviv_to_list(pm_data_lp, pm_data_lp_mapped, [])
    
    html_table_pm_banner = DR.IntervalReporting().write_html_table_from_rowlists(pm_data_banner_mapped, ['Payment Method', 'Portion of Donations'], 'Banner')
    html_table_pm_lp = DR.IntervalReporting().write_html_table_from_rowlists(pm_data_lp_mapped, ['Payment Method', 'Portion of Donations'], 'Landing Page')
    
    
    
    """ 
        BUILD REPORTING OBJECTS 
        =======================
    """
    
    if test_type == FDH._TESTTYPE_BANNER_:
        ir = DR.IntervalReporting(use_labels=use_labels_var,font_size=20,plot_type='step',query_type=FDH._QTYPE_BANNER_,file_path=projSet.__web_home__ + 'tests/static/images/')
        link_item = '<a href="http://meta.wikimedia.org/w/index.php?title=Special:NoticeTemplate/view&template=%s">%s</a>'
        measured_metric = ['don_per_imp', 'amt_norm_per_imp', 'click_rate'] 
                    
    elif test_type == FDH._TESTTYPE_LP_:
        ir = DR.IntervalReporting(use_labels=use_labels_var,font_size=20,plot_type='step',query_type=FDH._QTYPE_LP_, file_path=projSet.__web_home__ + 'tests/static/images/')
        link_item = '<a href="http://meta.wikimedia.org/w/index.php?title=Special:NoticeTemplate/view&template=%s">%s</a>'
        measured_metric = ['don_per_view', 'amt_norm_per_view']
        
    elif test_type == FDH._TESTTYPE_BANNER_LP_:
        ir = DR.IntervalReporting(use_labels=use_labels_var,font_size=20,plot_type='step',query_type=FDH._QTYPE_BANNER_LP_,file_path=projSet.__web_home__ + 'tests/static/images/')
        link_item = '<a href="http://meta.wikimedia.org/w/index.php?title=Special:NoticeTemplate/view&template=%s">%s</a>'
        measured_metric = ['don_per_imp', 'amt_norm_per_imp','don_per_view', 'amt_norm_per_view', 'click_rate']
    
    
    """ 
        GENERATE PLOTS FOR EACH METRIC OF INTEREST 
        ==========================================
    """
    for metric in metric_types:
        ir.run(start_time, end_time, sample_interval, metric, campaign, label_dict, one_step=one_step_var)
                    
    
    """ 
        CHECK THE CAMPAIGN VIEWS AND DONATIONS 
        ======================================
    """
    ir_cmpgn.run(start_time, end_time, sample_interval, 'views', campaign, {}, one_step=one_step_var)
    ir_cmpgn.run(start_time, end_time, sample_interval, 'donations', campaign, {}, one_step=one_step_var)
    
    
    """ 
        PERFORM HYPOTHESIS TESTING 
        ==========================
    """
    
    column_colours = dict()
    confidence = list()
    
    cr = DR.ConfidenceReporting(use_labels=use_labels_var,font_size=20,plot_type='line',hyp_test='t_test', query_type=test_type, file_path=projSet.__web_home__ + 'tests/static/images/')
    
    for metric in measured_metric:
        
        ret = cr.run(test_name, campaign, metric, label_dict, start_time, end_time, sample_interval, one_step=one_step_var)
        
        confidence.append(ret[0])
        column_colours[metric] = ret[1]
        
            
    """ 
        GENERATE A REPORT SUMMARY TABLE
        ===============================
    """
    
    if one_step_var == True:
        summary_start_time = DL.CiviCRMLoader().get_earliest_donation(campaign)
    else:
        summary_start_time = DL.LandingPageTableLoader().get_earliest_campaign_view(campaign)
    
    summary_end_time = DL.CiviCRMLoader().get_latest_donation(campaign)
    
    srl = DL.SummaryReportingLoader(test_type)
    srl.run_query(summary_start_time, summary_end_time, campaign, one_step=one_step_var)
    
    columns = srl.get_column_names()
    summary_results = srl.get_results()
        
    summary_results_list = list()
    
    """ Add label links """
    for row in summary_results:
        artifact_name = row[0]
        new_row = list(row)
        new_row[0] = link_item % (artifact_name, label_dict_full[artifact_name])
        summary_results_list.append(new_row)
    
    summary_results = summary_results_list
    
    """ Get Winners, Losers, and percent increase """
    
    winner = list()
    loser = list()
    percent_increase = list()
        
    labels = list()
    for item_long_name in label_dict:
        labels.append(label_dict[item_long_name])

    for metric in measured_metric:
        ret = srl.compare_artifacts(label_dict.keys(), metric, labels=labels)
        
        winner.append(ret[0])  
        loser.append(ret[1])
        percent_increase.append(ret[2])
    
    """ Compose table for showing artifact """
    html_table = DR.DataReporting()._write_html_table(summary_results, columns, coloured_columns=column_colours, use_standard_metric_names=True)    
    
    metric_legend_table = DR.DataReporting().get_standard_metrics_legend()
    conf_legend_table = DR.ConfidenceReporting(query_type='bannerlp', hyp_test='TTest').get_confidence_legend_table()
    
    html_table = '<h4><u>Metrics Legend:</u></h4><div class="spacer"></div>' + metric_legend_table + \
        '<div class="spacer"></div><h4><u>Confidence Legend for Hypothesis Testing:</u></h4><div class="spacer"></div>' + conf_legend_table + '<div class="spacer"></div><div class="spacer"></div>' + html_table
            
    """ Generate totals for the test summary """
    srl = DL.SummaryReportingLoader(FDH._QTYPE_TOTAL_)
    srl.run_query(summary_start_time, summary_end_time, campaign, one_step=one_step_var)
    html_table = html_table + '<br><br>' + DR.DataReporting()._write_html_table(srl.get_results(), srl.get_column_names(), use_standard_metric_names=True)
        

    return [measured_metric, winner, loser, percent_increase, confidence, html_table_pm_banner, html_table_pm_lp, html_language, html_table]


"""
    Produces a tabular summary for a given campaign given the start and end times
        
"""
def generate_summary(request):
    
    try:
        """ 
            PROCESS POST DATA
            ================= 
            
            Escape all user input that can be entered in text fields 
            
        """
        utm_campaign = MySQLdb._mysql.escape_string(request.POST['utm_campaign'])
        start_time = MySQLdb._mysql.escape_string(request.POST['start_time'])
        end_time = MySQLdb._mysql.escape_string(request.POST['end_time'])
            
        """ 
            GENERATE A REPORT SUMMARY TABLE
            ===============================
        """
        srl = DL.SummaryReportingLoader(FDH._TESTTYPE_BANNER_LP_)
        srl.run_query(start_time, end_time, utm_campaign, min_views=-1)    
        
        columns = srl.get_column_names()
        summary_results = srl.get_results()
            
        html_table = DR.DataReporting()._write_html_table(summary_results, columns)    
        
        """ Generate totals """
        srl = DL.SummaryReportingLoader(FDH._QTYPE_TOTAL_)
        srl.run_query(start_time, end_time, utm_campaign, min_views=-1)
        
        html_table = html_table + '<div class="spacer"></div><div class="spacer"></div>' + DR.DataReporting()._write_html_table(srl.get_results(), srl.get_column_names(), use_standard_metric_names=True)
        
        metric_legend_table = DR.DataReporting().get_standard_metrics_legend()
        conf_legend_table = DR.ConfidenceReporting(query_type='bannerlp', hyp_test='TTest').get_confidence_legend_table()
        
        html_table = '<h4><u>Metrics Legend:</u></h4><div class="spacer"></div>' + metric_legend_table + \
        '<div class="spacer"></div><h4><u>Confidence Legend for Hypothesis Testing:</u></h4><div class="spacer"></div>' + conf_legend_table + '<div class="spacer"></div><div class="spacer"></div>' + html_table
        
        return render_to_response('tests/table_summary.html', {'html_table' : html_table, 'utm_campaign' : utm_campaign}, context_instance=RequestContext(request))
    
    except:
        
        return render_to_response('tests/index.html', {'err_msg' : 'Could not generate campaign tabular results.'}, context_instance=RequestContext(request))
    
"""
    Inserts a comment into an existing report
        
"""
def add_comment(request, utm_campaign):
    
    try:
        comments = MySQLdb._mysql.escape_string(request.POST['comments'])
        
    except:
        return HttpResponseRedirect(reverse('tests.views.index'))
    
    """ Retrieve the report """
    ttl = DL.TestTableLoader()
    row = ttl.get_test_row(utm_campaign)
    html_string = ttl.get_test_field(row, 'html_report')
    
    """ Insert comment into the page html """
    new_html = ''
    lines = html_string.split('\n')
    now = datetime.datetime.utcnow().__str__()
    
    for line in lines:
        
        if line == '<!-- Cend -->':
            line = '<div class="spacer"></div>\n<div class="spacer"></div>\n' + comments + '\n<div class="spacer"></div>  --' + now + '\n<!-- Cend -->'
        
        new_html = new_html + line + '\n'
    
    html_string = new_html
    html_string = html_string.replace('"', '\\"')
    
    # parse the html for <!-- Cbegin --> <!-- Cend -->
    # add the comment above this
    
    """ Update the report """
    ttl.update_test_row(test_name=ttl.get_test_field(row, 'test_name'), test_type=ttl.get_test_field(row, 'test_type'), utm_campaign=ttl.get_test_field(row, 'utm_campaign'), start_time=ttl.get_test_field(row, 'start_time'), \
                        end_time=ttl.get_test_field(row, 'end_time'), winner=ttl.get_test_field(row, 'winner'), is_conclusive=ttl.get_test_field(row, 'is_conclusive'), html_report=html_string)

    
    return HttpResponse(new_html)

