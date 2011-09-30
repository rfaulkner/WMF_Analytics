"""

    Used for interfacing into particular filetypes for the framework
    
    particularly config files

"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "April 16th, 2011"

import commands, gzip, re, os, logging

""" CONFIGURE THE LOGGER """
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


"""

    BASE CLASS :: FileHandler
    
    General file parsing functionality
    
    METHODS:
        
        
"""
class FileHandler(object):
    
    def __init__(self):
        return


"""

    CLASS :: ConfigFileHandler
    
    Parse a configuration file for reporting framework
    
    METHODS:
        
        
"""
class LogFileHandler(object):
    
    _log_dir_ = '/a/static/uncompressed/udplogs/'
    
    def __init__(self):
        
        self._total_lines_in_file_ = -1 
        self._log_file_ = None
        
        self._line_delimeter_ = ' '
        self._line_index_ = 0
        self._pattern_list_ = ''
    
    def __del__(self):        
        try:
            self._log_file_.close()
        except:
            pass
    
    """
    """
    def set_proc_params(self, delimeter, indices, patterns):
        
        self._line_delimeter_ = delimeter
        self._line_index_list_ = indices
        self._pattern_list_ = patterns
        
    
    """
        Determine if a squid log exists by checking in the squid home directory - this assumes that all files loaded into this folder 
        have been successfully mined
    """
    def log_exists(self, log_name):
        
        files = os.listdir(self._log_dir_)
        files.sort()
        
        for f in files:
            """ Is the log name key a substring of the file name """
            if re.search(log_name, f):
                return True
            
        return False
    
    """
        Determine if a squid log exists by checking in the squid home directory - this assumes that all files loaded into this folder 
        have been successfully mined
    """
    def get_logfiles_from_pattern(self, pattern):
        
        files = os.listdir(self._log_dir_)
        files.sort()
        
        file_list = list()
        
        for f in files:
            """ Is the log name key a substring of the file name """
            if re.search(pattern, f):
                file_list.append(f)
            
        return file_list
     
    """
        Open a file for reading 
    """
    def open_file(self, filename):
        
        try:
            self._log_file_.close()
        except:
            pass

        filematch = re.match(r"([0-9a-zA-Z_.-]+)", filename)
        if filematch:
            filename = filematch.group(0)
            if re.search('\.gz', filename):
                self._log_file_ = gzip.open(self._log_dir_ + filename, 'r')
                self._total_lines_in_file_ = float(commands.getstatusoutput('zgrep -c "" ' + self._log_dir_ + filename)[1])
            else:
                self._log_file_ = open(self._log_dir_ + filename, 'r')
                self._total_lines_in_file_ = float(commands.getstatusoutput('grep -c "" ' + self._log_dir_ + filename)[1])

        
    """
        Read through the file in the current 
    """
    def read_file(self, filename):
        
        try:
            self.open_file(filename)
        except IOError:
            logging.error('Could not open the file: %s' % filename)
            
        line_count = 0        
        line = self._log_file_.readline()
        
        matching_lines = list()
        
        while (line != ''):
            
            line_match = self.proc_line(line)
            if line_match:
                line_match['line_num'] = line_count
                matching_lines.append(line_match)
                
            line = self._log_file_.readline()
            line_count = line_count + 1
        
        return matching_lines
        
    """
        Process a line of the log file
    """
    def proc_line(self, line):
        
        matches = dict()
        line_parts = line.split(self._line_delimeter_)
        
        for index in range(len(line_parts)):
            
            if index in self._line_index_list_:
                for pattern in self._pattern_list_:
                    if re.search(pattern, line_parts[index]):
                        matches[index] = line_parts[index]
                        
        return matches
    