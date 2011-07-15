
"""

This module is used to define the mapping between data sources.  The primary intention is to convert Squid log representations into 
MySQL database representations however this task may grow over time.

The DataMapper class decouples the data mapping function of from the loading and reporting using Adapter structural pattern.

"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "April 25th, 2011"


""" Import python base modules """
import sys, urlparse as up, httpagentparser, commands, cgi, re, gzip, os, datetime, logging

""" Import Analytics modules """
import Fundraiser_Tools.classes.DataLoader as DL
import Fundraiser_Tools.classes.Helper as Hlp
import Fundraiser_Tools.settings as projSet
import Fundraiser_Tools.classes.TimestampProcessor as TP
import Fundraiser_Tools.classes.FundraiserDataHandler as FDH

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

"""

    BASE CLASS :: DataMapper
    
    Base class for interacting with DataSource.  Methods that are intended to be extended in derived classes include:
    
    METHODS:
        
        copy_logs         - copies logs from source to destination for processing for the current hour
        log_exists        - determines if a log exists in the squid local folder based on name
        get_list_of_logs  - returns a list of logs in the squid local folder 

"""
class DataMapper(object):
    
    _LP_LOG_PREFIX_ = 'bannerImpressions' 
    _BANNER_LOG_PREFIX_ = 'landingpages'
    
    _log_copy_interval_ = 15
    _log_poll_interval_ = 1
    
         
    """
        Copies mining logs from remote site for a given hour
        
        @param type: specifies whether the log contains banner or landing page requests
        @type type: string
    """
    def copy_logs(self, type, **kwargs):
        
        if type == FDH._TESTTYPE_BANNER_:
            prefix = self._LP_LOG_PREFIX_
            
        elif type == FDH._TESTTYPE_LP_:
            prefix = self._BANNER_LOG_PREFIX_
        
        filename = prefix
        
        now = datetime.datetime.now()
        year = str(now.year)
        month = str(now.month)
        day = str(now.day)
        hour = now.hour
        
        """ If specified change the timestamp  Assume each arg is a string """
        for key in kwargs:
            
            if key == 'year':
                year = kwargs[key]
            elif key == 'month':                
                month = kwargs[key]
            elif key == 'day':
                day = kwargs[key]
            elif key == 'hour':
                hour = int(kwargs[key])
        
        if int(month) < 10:
            month = '0' + str(int(month))
        if int(day) < 10:
            day = '0' + str(int(day))
            
        """ adjust the hour based on time of day """ 
        if hour > 12:
            hour = str(hour-12)
            day_part = 'PM'
        else:
            hour = str(hour)
            day_part = 'AM'
        
        if int(hour) < 10:
            hour = '0' + str(int(hour))
            
        filename = filename + '-' + year + '-' + month + '-' + day + '-' + hour + day_part

        copied_logs = list()
        """ Try to load each log for that hour - only load gzipped logs for now """
        for i in range(4):
            minute = i * 15
            
            if minute >= 10:
                minute = str(minute) + '.log.gz'
            else:
                minute = '0' + str(minute) + '.log.gz'
            
            full_filename = filename  + '--' + minute
            
            cmd = 'sftp ' + projSet.__user__ + '@' + projSet.__squid_log_server__ + ':' + projSet.__squid_log_home__ + full_filename + ' ' + projSet.__squid_log_local_home__ 
            
            if not(self.log_exists(full_filename)):            
                os.system(cmd)
                copied_logs.append(full_filename)
            else:
                logging.info('File: %s has already been loaded.' % full_filename)
                
        return copied_logs

    """
        Return a listing of all of the squid logs
    """
    def get_list_of_logs(self):
        
        files = os.listdir(projSet.__squid_log_local_home__)
        files.sort()
        
        new_files = list()
        for f in files:
            new_files.append(f.split('.')[0])
            
        return new_files[1:]
        
    """
        Determine if a squid log exists
    """
    def log_exists(self, log_name):
        
        files = os.listdir(projSet.__squid_log_local_home__)
        files.sort()
        
        for f in files:
            if f == log_name:
                return True
            
        return False
    
    
    """
        Retrieve the timestamp of the latest log
    """
    def get_time_of_last_log(self):
        
        log_names = self.get_list_of_logs()
        
        """ In case directory initialize datetime object to one day in the past  """
        log_time_obj = datetime.datetime.now() + datetime.timedelta(days=-1)
        
        for name in log_names:
        
            """ Retrieve the log timestamp from the filename and convert to datetime objects """
            time_stamps = self.get_timestamps_with_interval(name, self._log_copy_interval_)
            log_end_time_obj = TP.timestamp_to_obj(time_stamps[1], 1)
            
            if name == log_names[0]:
                log_time_obj = log_end_time_obj 
            else:
                if log_end_time_obj > log_time_obj:
                    log_time_obj = log_end_time_obj
        
        return log_time_obj
    
    """ 
        Extract a timestamp from the squid log filename given the interval length over which the contained requests are logged 
    """    
    def get_timestamps_with_interval(self, logFileName, interval):
    
        log_end = self.get_timestamps(logFileName)[1]
        
        end_obj = TP.timestamp_to_obj(log_end, 1)
        start_obj = end_obj + datetime.timedelta(minutes=-interval)
        
        start_timestamp = TP.timestamp_from_obj(start_obj, 1, 2)
        end_timestamp = TP.timestamp_from_obj(end_obj, 1, 2)
        
        return [start_timestamp, end_timestamp]
        
        
    """ 
        Extract a timestamp from the squid log filename
        
        e.g. landingpages-2011-07-01-09PM--00.log.gz
             bannerImpressions-2011-07-01-09PM--00.log.gz
             
    """
    def get_timestamps(self, logFileName):
        
        fname_parts = logFileName.split('-')
    
        year = int(fname_parts[1])
        month = int(fname_parts[2])
        day = int(fname_parts[3])
        hour = int(fname_parts[4][0:2])
        minute = int(fname_parts[6][0:2])
        
        # Is this an afternoon log?
        afternoon = (fname_parts[4][2:4] == 'PM') 
         
        # Adjust the hour as necessary if == 12AM or *PM
        if afternoon and hour < 12:
            hour = hour + 12
            
        if not(afternoon) and hour == 12:
            hour = 0
    
        prev_hr = TP.getPrevHour(year, month, day, hour)
        
        str_month = '0' + str(month) if month < 10 else str(month)
        str_day = '0' + str(day) if day < 10 else str(day)
        str_hour = '0' + str(hour) if hour < 10 else str(hour)
        str_minute = '0' + str(minute) if minute < 10 else str(minute)
        
        prev_month = prev_hr[1] 
        prev_day = prev_hr[2]
        prev_hour = prev_hr[3]
        str_prev_month = '0' + str(prev_month) if prev_month < 10 else str(prev_month)
        str_prev_day = '0' + str(prev_day) if prev_day < 10 else str(prev_day)
        str_prev_hour = '0' + str(prev_hour) if prev_hour < 10 else str(prev_hour)
        
        log_end = str(year) + str_month + str_day + str_hour + str_minute + '00'
        log_start = str(prev_hr[0]) + str_prev_month + str_prev_day + str_prev_hour + '5500' 
            
        return [log_start, log_end]
        
        
        
"""

    CLASS :: FundraiserDataMapper
    
    Data mapper specific to the Wikimedia Fundraiser
    
    METHODS:
        
        mine_squid_impression_requests        - mining banner impressions from squid logs
        mine_squid_landing_page_requests      - mining landing page views from squid logs

"""
class FundraiserDataMapper(DataMapper):
                
    _BANNER_REQUEST_ = 0
    _LP_REQUEST_ = 1
    
    _BANNER_FIELDS_ =  ' (start_timestamp, utm_source, referrer, country, lang, counts, on_minute) '
    _LP_FIELDS_ = ' (start_timestamp, utm_source, utm_campaign, utm_medium, landing_page, page_url, referrer_url, browser, lang, country, project, ip, request_time) '
    
    def __init__(self):
        
        """ Initialize dataloaders and connections """
        self._DL_impressions_ = DL.ImpressionTableLoader()
        self._DL_LPrequests_ = DL.LandingPageTableLoader()
        
        self._DL_impressions_.init_db()
        self._DL_LPrequests_.init_db()
        
    
    """
        Determines if new logs may be waiting - if so they are copied and mined
    """
    def poll_logs(self):
        
        time_of_last_log = self.get_time_of_last_log()
        curr_time = datetime.datetime.now()
        
        logging.debug('Time of last log: %s' % str(time_of_last_log))
        logging.debug('Current Time: %s' % str(curr_time))
        
        if time_of_last_log + datetime.timedelta(minutes=self._log_copy_interval_) < curr_time:
            
            """ Copy over the latest logs """
            copied_banner_logs = self.copy_logs('banner',year=str(curr_time.year), month=str(curr_time.month), day=str(curr_time.day), hour=str(curr_time.hour))
            copied_lp_logs = self.copy_logs('lp',year=str(curr_time.year), month=str(curr_time.month), day=str(curr_time.day), hour=str(curr_time.hour))
                        
            """ Mine the latest logs """
            for banner_imp_file in copied_banner_logs:
                try:
                    self.mine_squid_impression_requests(banner_imp_file)
                except IOError as inst:
                    logging.error(inst)
                    logging.error('Could not mine contents of %s, it appears that it does not exist. ' % banner_imp_file)
                    
            for lp_view_file in copied_lp_logs:
                try:
                    self.mine_squid_landing_page_requests(lp_view_file)
                except IOError as inst:
                    logging.error(inst)
                    logging.error('Could not mine contents of %s, it appears that it does not exist. ' % lp_view_file)
    
     
    """
        Remove banner impression or landing page squid records from tables before loading 
    """
    def _clear_squid_records(self, start, request_type):
        
        """ Ensure that the range is correct; otherwise abort - critical that outside records are not deleted """
        timestamp = TP.timestamp_convert_format(start,1,2)
        
        try:
            if request_type == self._BANNER_REQUEST_:
                self._DL_impressions_.delete_row(timestamp)
            elif request_type == self._LP_REQUEST_:
                self._DL_LPrequests_.delete_row(timestamp)
            
            logging.info("Executed delete for start time " + timestamp)
        
        except Exception as inst:
            
            logging.error("Could not execute delete for start time " + timestamp) 
            
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)     # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            """ Die if the records cannot be removed """
            sys.exit()

        
    """
        Given the name of a log file extract the squid requests corresponding to banner impressions.
        
        Squid logs can be found under hume:/a/static/uncompressed/udplogs.  A sample request is documented in the source below.
        
        @param logFileName: the full name of the logfile.  The local squid log folder is stored in web_reporting/settings.py
        @type logFileName: string
        
    """
    def mine_squid_impression_requests(self, logFileName):
        
        # self._init_db()

        sltl = DL.SquidLogTableLoader()
        itl = DL.ImpressionTableLoader()
        
        """ Retrieve the log timestamp from the filename """
        time_stamps = self.get_timestamps_with_interval(logFileName, self._log_copy_interval_)
        
        start = time_stamps[0]
        end = time_stamps[1]
        start_timestamp_in = "convert(\'" + start + "\', datetime)"
        curr_time = TP.timestamp_from_obj(datetime.datetime.now(),1,3)
        
        
        """ retrieve the start time of the log """
        start = self.get_first_timestamp_from_log(logFileName)
        
        """ Initialization - open the file """
        logFile, total_lines_in_file = self.open_logfile(logFileName)
        
        
        queryIndex = 4;
    
        counts = Hlp.AutoVivification()
        # insertStmt = 'INSERT INTO ' + self._impression_table_name_ + self._BANNER_FIELDS_ + ' values '
    
        min_log = -1
        hr_change = 0
        clamp = 0
        
        """ Clear the old records """
        self._clear_squid_records(start, self._BANNER_REQUEST_)
        
        """ Add a row to the SquidLogTable """
        sltl.insert_row(type='banner_impression',log_copy_time=curr_time,start_time=start,end_time=end,log_completion_pct='0.0',total_rows='0')
        
        """
            PROCESS LOG FILE
            ================
            
            Sample Request:
            
            line =
            "sq63.wikimedia.org 757675855 2011-06-01T23:00:07.612 0 187.57.227.121 TCP_MEM_HIT/200 1790 GET \
            http://meta.wikimedia.org/w/index.php?title=Special:BannerLoader&banner=B20110601_JWJN001_BR&userlang=pt&db=ptwiki&sitename=Wikip%C3%A9dia&country=BR NONE/- text/javascript http://pt.wikipedia.org/wiki/Modo_e_tempo_verbal \
            - Mozilla/5.0%20(Windows%20NT%206.1)%20AppleWebKit/534.24%20(KHTML,%20like%20Gecko)%20Chrome/11.0.696.71%20Safari/534.24"

        """
        
        line_count = 0
        line = logFile.readline()
        while (line != ''):
    
            lineArgs = line.split()
            
            """ 
                Parse the Timestamp:
                
                Sample timestamp:
                    timestamp = "2011-06-01T23:00:07.612"
            """
            
            try:
                time_stamp = lineArgs[2]
                time_bits = time_stamp.split('T')
                date_fields = time_bits[0].split('-')
                time_fields = time_bits[1].split(':')
                minute = int(time_fields[1])
            except (ValueError, IndexError):
                line = logFile.readline()
                total_lines_in_file = total_lines_in_file - 1
                continue
                # pass
            
            """ Logic used to deal with logs that aren't sequential """
    
            if minute == 0 and not(hr_change) and not(clamp):
                min_log = -1
    
            if minute == 1:
                hr_change = 0
                clamp = 1
    
            """ 
                Parse the URL:
                
                Sample url:
                    url = "http://meta.wikimedia.org/w/index.php?title=Special:BannerLoader&banner=B20110601_JWJN001_BR&userlang=pt&db=ptwiki&sitename=Wikip%C3%A9dia&country=BR"
            """
    
            try:
                url = lineArgs[8]
            except IndexError:
                url = 'Unavailable'
    
            parsedUrl = up.urlparse(url)
            query = parsedUrl[queryIndex]
            queryBits = cgi.parse_qs(query)
    
            """ Extract - project, banner, language, & country data from the url """
            project = ''
            if ('db' in queryBits.keys()):
                project = queryBits['db'][0]
    
            if (project == '' and 'sitename' in queryBits.keys()):
                sitename = queryBits['sitename'][0];
                if sitename:
                    project = sitename
                else:
                    project = 'NONE'
    
            if ('banner' in queryBits.keys()):
                banner = queryBits['banner'][0]
            else:
                banner = 'NONE'
    
            if ('userlang' in queryBits.keys()):
                lang = queryBits['userlang'][0]
            else:
                lang = 'NONE'
    
            if ('country' in queryBits.keys()):
                country = queryBits['country'][0]
            else:
                country = 'NONE'
    
            """ Group banner impression counts based on (banner, country, project, language) """
            try:
                counts[banner][country][project][lang] = counts[banner][country][project][lang] + 1
            except TypeError:
                counts[banner][country][project][lang] = 1
    

            """ 
                Break out impression data by minute.  This conditional detects when a request with a previously unseen minute in the timestamp appears.
                
            """
            if min_log < minute and not(hr_change):
                
                if minute == 0:
                    hr_change = 1
    
                min_log = minute
                time_stamp_in = "convert(\'" + date_fields[0] + date_fields[1] + date_fields[2] + time_fields[0] + time_fields[1]  + "00\', datetime)"

    
                """ Run through the counts dictionary and insert a row into the banner impressions table for each entry """
    
                bannerKeys = counts.keys()
                for banner_ind in range(len(bannerKeys)):
                    banner = bannerKeys[banner_ind]
                    countryCounts = counts[banner]
                    countryKeys = countryCounts.keys()
    
                    for country_ind in range(len(countryKeys)):
                        country = countryKeys[country_ind]
                        projectCounts = countryCounts[country]
                        projectKeys = projectCounts.keys()
    
                        for project_ind in range(len(projectKeys)):
                            project = projectKeys[project_ind]
                            langCounts = projectCounts[project]
                            langKeys = langCounts.keys()
    
                            for lang_ind in range(len(langKeys)):
                                lang = langKeys[lang_ind]
                                count = langCounts[lang]
                                
                                itl.insert_row(utm_source_arg=banner, referrer_arg=project, country_arg=country, lang_arg=lang, counts_arg=str(count), on_minute_arg=time_stamp_in, start_timestamp_arg=start_timestamp_in)

    
                # Re-initialize counts
                counts = Hlp.AutoVivification()

            line = logFile.readline()
            line_count = line_count + 1
            
            """ Log Miner Logging - Update the squid_log_record table """
            if line_count % 10000 == 0 or line_count == total_lines_in_file:
                completion = float(line_count / total_lines_in_file) * 100.0
                sltl.update_table_row(type='banner_impression',log_copy_time=curr_time,start_time=start,end_time=end,log_completion_pct=completion.__str__(),total_rows=line_count.__str__())

        
        logFile.close()



    """
        Given the name of a log file Extract the squid requests corresponding to landing page views 
        
        Squid logs can be found under hume:/a/static/uncompressed/udplogs.  A sample request is documented in the source below.
        
        @param logFileName: the full name of the logfile.  The local squid log folder is stored in web_reporting/settings.py
        @type logFileName: string

    """
    def mine_squid_landing_page_requests(self,  logFileName):

        #self._init_db()
        """ Create the dataloaders and initialize """
        sltl = DL.SquidLogTableLoader()
        lptl = DL.LandingPageTableLoader()
        ipctl = DL.IPCountryTableLoader()
        
        """ Retrieve the log timestamp from the filename """
        #time_stamps = Hlp.get_timestamps(logFileName)
        time_stamps = self.get_timestamps_with_interval(logFileName, self._log_copy_interval_)
        
        start = time_stamps[0]
        end = time_stamps[1]
        start_timestamp_in = "convert(\'" + start + "\', datetime)"
        curr_time = TP.timestamp_from_obj(datetime.datetime.now(),1,3)
        
        """ retrieve the start time of the log """
        start = self.get_first_timestamp_from_log(logFileName)
        
        """ Initialization - open the file """
        logFile, total_lines_in_file = self.open_logfile(logFileName)
    
        # Initialization
        hostIndex = 1;
        queryIndex = 4;
        pathIndex = 2;
    
        """ Clear the old records """
        self._clear_squid_records(start, self._LP_REQUEST_)
        
        """ Add a row to the SquidLogTable """
        sltl.insert_row(type='lp_view',log_copy_time=curr_time,start_time=start,end_time=end,log_completion_pct='0.0',total_rows='0')

        line_count = 0
        
        """
            PROCESS REQUESTS FROM FILE
            ==========================
            
            Sample request:
            
            line = 
                "sq63.wikimedia.org 757671483 2011-06-01T23:00:01.343 93 98.230.113.246 TCP_MISS/200 10201 GET \
                http://wikimediafoundation.org/w/index.php?title=WMFJA085/en/US&utm_source=donate&utm_medium=sidebar&utm_campaign=20101204SB002&country_code=US&referrer=http%3A%2F%2Fen.wikipedia.org%2Fwiki%2FFile%3AMurphy_High_School.jpg CARP/208.80.152.83 text/html http://en.wikipedia.org/wiki/File:Murphy_High_School.jpg \
                - Mozilla/4.0%20(compatible;%20MSIE%208.0;%20Windows%20NT%206.1;%20WOW64;%20Trident/4.0;%20FunWebProducts;%20GTB6.6;%20SLCC2;%20.NET%20CLR%202.0.50727;%20.NET%20CLR%203.5.30729;%20.NET%20CLR%203.0.30729;%20Media%20Center%20PC%206.0;%20HPDTDF;%20.NET4.0C)"
        """
        line = logFile.readline()
        while (line != ''):
            lineArgs = line.split()
    
            """ Get the IP Address of the donor """
            ip_add = lineArgs[4];
    
    
            #  SELECT CAST('20070529 00:00:00' AS datetime)

            """ 
                Parse the Timestamp:
                
                Sample timestamp:
                    timestamp = "2011-06-01T23:00:07.612"
            """

    
            date_and_time = lineArgs[2];
    
            date_string = date_and_time.split('-')
            time_string = date_and_time.split(':')
    
            # if the date is not logged ignoere the record
            try:
                year = date_string[0]
                month = date_string[1]
                day = date_string[2][:2]
                hour = time_string[0][-2:]
                min = time_string[1]
                sec = time_string[2][:2]
            except:
                line = logFile.readline()
                total_lines_in_file = total_lines_in_file - 1
                continue
    
            timestamp_string = year + '-' + month + '-' + day + " " + hour + ":" + min + ":" + sec                
    
            """ 
                Process referrer URL
                =================== 
                
                Sample referrer:
                    referrer_url = http://en.wikipedia.org/wiki/File:Murphy_High_School.jpg
            """
                
            try:
                referrer_url = lineArgs[11]
            except IndexError:
                referrer_url  = 'Unavailable'
                
            parsed_referrer_url = up.urlparse(referrer_url)
    
            if (parsed_referrer_url[hostIndex] == None):
                project = 'NONE';
                source_lang = 'NONE';
            else:
                hostname = parsed_referrer_url[hostIndex].split('.')
                
                """ If the hostname of the form '<lang>.<project>.org' """
                if ( len( hostname[0] ) <= 2 ) :
                    # referrer_path = parsed_referrer_url[pathIndex].split('/')
                    project = hostname[0]                  # wikimediafoundation.org
                    source_lang = hostname[0]
                else:
                    try:
                        """ species.wikimedia vs en.wikinews """
                        project = hostname[0] if ( hostname[1] == 'wikimedia' ) else hostname[1]    
                        """ pl.wikipedia vs commons.wikimedia """
                        source_lang = hostname[0] if ( len(hostname[1]) < 5 ) else 'en'             
                    except:
                        project = 'wikipedia'   """ default project to 'wikipedia' """
                        source_lang = 'en'      """ default lang to english """
            
            """
                Process User agent string
                ========================
                
                sample user agent string:
                    user_agent_string = Mozilla/4.0%20(compatible;%20MSIE%208.0;%20Windows%20NT%206.1;%20WOW64;%20Trident/4.0;%20FunWebProducts;%20GTB6.6;%20SLCC2;%20.NET%20CLR%202.0.50727;%20.NET%20CLR%203.5.30729;%20.NET%20CLR%203.0.30729;%20Media%20Center%20PC%206.0;%20HPDTDF;%20.NET4.0C)
                
            """
            
            try:
                user_agent_string = lineArgs[13]
            except IndexError:
                user_agent_string = ''
                
            user_agent_fields = httpagentparser.detect(user_agent_string)
            browser = 'NONE'
    
            # Check to make sure fields exist
            if len(user_agent_fields['browser']) != 0:
                if len(user_agent_fields['browser']['name']) != 0:
                    browser = user_agent_fields['browser']['name']
    
            """
                 Process landing URL
                 ===================
                 
                 sample landing url
                     landing_url = "http://wikimediafoundation.org/w/index.php?title=WMFJA085/en/US&utm_source=donate&utm_medium=sidebar&utm_campaign=20101204SB002&country_code=US&referrer=http%3A%2F%2Fen.wikipedia.org%2Fwiki%2FFile%3AMurphy_High_School.jpg"
                     landing_url = "http://wikimediafoundation.org/wiki/WMFJA1/ru"
            """
            
            try:
                landing_url = lineArgs[8]
            except IndexError:
                landing_url = 'Unavailable'
                
            hostIndex = 1
            queryIndex = 4
            pathIndex = 2
            
            parsed_landing_url = up.urlparse(landing_url)
            query_fields = cgi.parse_qs(parsed_landing_url[queryIndex]) # Get the banner name and lang
            path_pieces = parsed_landing_url[pathIndex].split('/')

            include_request, index_str_flag = self.evaluate_landing_url(landing_url, parsed_landing_url, query_fields, path_pieces)
            
            if include_request:
                
                """ Address cases where the query string contains the landing page - ...wikimediafoundation.org/w/index.php?... """
                if index_str_flag:
                    try:
                        
                        """ URLs of the form ...?title=<lp_name> """
                        lp_country = query_fields['title'][0].split('/')
                        landing_page = lp_country[0]
                        
                        """ URLs of the form ...?county_code=<iso_code> """
                        try:
                            country = query_fields['country_code'][0]
                        except:
                            """ URLs of the form ...?title=<lp_name>/<lang>/<iso_code> """
                            if len(lp_country) == 3:
                                country = lp_country[2]
                            else:
                                country = lp_country[1]
                            
                    except:
                        landing_page = 'NONE'
                        country = ipctl.localize_IP(ip_add) 
                        
                else: 
                    """ Address cases where the query string does not contain the landing page - ...wikimediafoundation.org/wiki/... """
                    parsed_landing_url = up.urlparse(landing_url)
                    query_fields = cgi.parse_qs(parsed_landing_url[queryIndex]) # Get the banner name and lang
                    
                    landing_path = parsed_landing_url[pathIndex].split('/')
                    landing_page = landing_path[2];
                    
                    # URLs of the form ...?county_code=<iso_code>
                    try:
                        country = query_fields['country_code'][0]
                    
                    # URLs of the form ...<path>/ <lp_name>/<lang>/<iso_code>
                    except:
                        try:
                            if len(landing_path) == 5:
                                country = landing_path[4] 
                                # source_lang = landing_path[3]                             
                            else:
                                country = landing_path[3]
                                
                        except:
                            country = ipctl.localize_IP(ip_add)
                
                # If country is confused with the language use the ip
                if country == country.lower():
                    country = ipctl.localize_IP(ip_add) 
                                
                # ensure fields exist
                try:
                    utm_source = query_fields['utm_source'][0]
                    utm_campaign = query_fields['utm_campaign'][0]
                    utm_medium = query_fields['utm_medium'][0];
    
                except KeyError:
                    utm_source = 'NONE'
                    utm_campaign = 'NONE'
                    utm_medium = 'NONE'
                
                """ Insert record into the landing_page_requests table """
                
                lptl.insert_row(utm_source_arg=utm_source, utm_campaign_arg=utm_campaign, utm_medium_arg=utm_medium, landing_page_arg=landing_page, page_url_arg=landing_url, \
                    referrer_url_arg=referrer_url, browser_arg=browser, lang_arg=source_lang, country_arg=country, project_arg=project, ip_arg=ip_add, start_timestamp_arg=start_timestamp_in, timestamp_arg=timestamp_string)
                
            line = logFile.readline()
            line_count = line_count + 1 
    
            """ Log Miner Logging - Update the squid_log_record table """
            if (line_count % 1000) == 0 or line_count == total_lines_in_file:
                completion = float(line_count / total_lines_in_file) * 100.0
                sltl.update_table_row(type='lp_view',log_copy_time=curr_time,start_time=start,end_time=end,log_completion_pct=completion.__str__(),total_rows=line_count.__str__())

        #self._close_db()
        

    """
        Looks into the logfile and pull the timestamp of the first request
        
        @param logFileName: the full name of the logfile.  The local squid log folder is stored in web_reporting/settings.py
        @type logFileName: string
        
    """
    def get_first_timestamp_from_log(self, logFileName):
        
        logFile = self.open_logfile(logFileName)[0]
        
        """ Scan through the log file """
        line = logFile.readline()
        while (line != ''):
            lineArgs = line.split()
            
            """ Try to extract Timestamp data """
            try:
                
                time_stamp = lineArgs[2]
                time_bits = time_stamp.split('T')
                date_fields = time_bits[0].split('-')
                time_fields = time_bits[1].split(':')
            
            except (ValueError, IndexError):
                
                line = logFile.readline()
                continue
                
            """ Break the loop once we have a timestamp """
            first_time_stamp = date_fields[0] + date_fields[1] + date_fields[2] + time_fields[0] + time_fields[1]  + '00'
            break
                
        logFile.close()
        
        return first_time_stamp
    


    
    """
        Opens the logfile and counts the total number of lines
        
        @param logFileName: the full name of the logfile.  The local squid log folder is stored in web_reporting/settings.py
        @type logFileName: string
        
    """
    def open_logfile(self, logFileName):        
        
        if (re.search('\.gz', logFileName)):
            logFile = gzip.open(projSet.__squid_log_local_home__ + logFileName, 'r')
            total_lines_in_file = float(commands.getstatusoutput('zgrep -c "" ' + projSet.__squid_log_local_home__ + logFileName)[1])
        else:
            logFile = open(projSet.__squid_log_local_home__ + logFileName, 'r')
            total_lines_in_file = float(commands.getstatusoutput('grep -c "" ' + projSet.__squid_log_local_home__ + logFileName)[1])

        return logFile, total_lines_in_file



    """
        Parses the landing url and determines if its valid
        
        @param landing_url: full landing page url
        @type landing_url: string
        
        @param parsed_landing_url: landing_url parsed into components
        @type parsed_landing_url: dictionary
        
        @param query_fields: query string field values
        @type query_fields: dictionary
        
        @param path_pieces: url path components
        @type path_pieces: list
        
    """
    def evaluate_landing_url(self, landing_url, parsed_landing_url, query_fields, path_pieces):        
        
        hostIndex = 1
        #queryIndex = 4
        #pathIndex = 2

        """ 
            Filter the landing URLs
        
             /wikimediafoundation.org/wiki/WMF/
             /wikimediafoundation.org/w/index.php?title=WMF/ 

            Evaluate conditions which determine acceptance of request based on the landing url 
        """
        try: 
            c1 = re.search('WMF', path_pieces[2] ) != None or re.search('Junetesting001', path_pieces[2] ) != None or re.search('L11', path_pieces[2] ) 
            c2 = re.search('Hear_from_Kartika', path_pieces[2]) != None
            
            cond1 = parsed_landing_url[hostIndex] == 'wikimediafoundation.org' and path_pieces[1] == 'wiki' and (c1 or c2)

            c1 = re.search('index.php', path_pieces[2] )  != None
            index_str_flag = c1
            
            try:
                c2 = re.search('WMF', query_fields['title'][0] ) != None or re.search('L2011', query_fields['title'][0] ) != None  or re.search('L11', query_fields['title'][0] ) != None 
            except KeyError:
                c2 = 0
            cond2 = (parsed_landing_url[hostIndex] == 'wikimediafoundation.org' and path_pieces[1] == 'w' and c1 and c2)
                            
            regexp_res = re.search('Special:LandingCheck',landing_url)
            cond3 = (regexp_res == None)
            
            return [(cond1 or cond2) and cond3, index_str_flag]

        except: 
            #print type(e)     # the exception instance
            #print e.args      # arguments stored in .args
            #print e           # __str__ allows args to printed directly
            
            return [False, False]

    