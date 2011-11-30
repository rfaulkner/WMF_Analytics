"""

This module defines functionality to coordinate Analytics processes with system resources

"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "November 29th, 2011"


""" Import python base modules """
import sys, os, re, logging, datetime

import config.settings as projSet

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

"""

    BASE CLASS :: SystemMonitor
    
    Defines the basic functionality of a caching class - storing and retrieving data from a serialized object
                   
"""
class SystemMonitor(object):
    
    def __init__(self):            
        return
    
    """
        Add a process to the PID file
    """
    def add_process_to_PIDfile(self, user, cmd_pattern, pid, start_time):
        
        pidfile = open(projSet.__data_file_dir__ + projSet.__PID_FILE__, 'a')
        new_entry = str(user) + ' ' + str(cmd_pattern) + ' ' + str(pid) + ' ' + str(start_time)
        pidfile.write(new_entry + '\n')
        pidfile.close()
        
        logging.info('Added to PID file: "%s"' % new_entry.strip())
        
    
    """
        Returns false in the case that the process already exists and true otherwise
    """    
    def check_for_process_in_PIDfile(self, user, cmd_pattern):
        
        PID = str(os.getpid())
        PIDfile = open(projSet.__data_file_dir__ + projSet.__PID_FILE__, 'r')
        now = datetime.datetime.utcnow()
                
        line = PIDfile.readline()
        while (line != ''):
            if re.search(user, line) and re.search(cmd_pattern, line):
                logging.info('Process running this script already exists: "%s"' % line.strip())
                return False
            
            line = PIDfile.readline()
        
        PIDfile.close()
        
        self.add_process_to_PIDfile(user, cmd_pattern, PID, now)
        
        return True
    
    
    """
        Remove a process to the PID file
    """
    def remove_process_from_PIDfile(self, user, cmd_pattern):
        
        PIDfile = open(projSet.__data_file_dir__ + projSet.__PID_FILE__, 'r')
        removed_entry = ''
        
        line = PIDfile.readline()
        new_lines = list()
        while (line != ''):
            
            if re.search(user, line) and re.search(cmd_pattern, line):
                removed_entry = line.strip()
                logging.info('Found process to remove in PID file: "%s"' % removed_entry)                 
            elif cmp(line, '') != 0:
                new_lines.append(line)
                
            line = PIDfile.readline()
        
        PIDfile = open(projSet.__data_file_dir__ + projSet.__PID_FILE__, 'w')
        for line in new_lines:
            PIDfile.write(line)
        
        if removed_entry:
            logging.info('Removed "%s" from PID file' % removed_entry)
        
        return False
    