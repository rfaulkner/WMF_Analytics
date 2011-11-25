
"""
    Script to find and kill outstanding procs in a batch 
"""


""" Script meta """
__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "November 17th, 2011"


""" Import python base modules """
import sys, os, logging, re
import settings as projSet
sys.path.append(projSet.__project_home__)

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

"""
    Define script usage
"""
class Usage(Exception):
    
    def __init__(self, msg):
        self.msg = msg

"""
    Execution body of main
"""
def main(args):
    
    result = os.popen('ps -A -F | grep rfaulk')

    line = result.readline()
    proc_list = list()
    
    user_index = 0
    ID_index = 1
    # time_index = 9

    """ Search for all python processes on user 'rfaulk' - don't kill yourself ;) """
    while(line):     
        proc_data = line.split()   
        if re.search('python', line) and (cmp('rfaulk', proc_data[user_index]) == 0) and not(re.search('run_kill_all_procs', line)):
            proc_list.append(proc_data)
        line = result.readline()
        
                        
    """ Kill all sleeping threads and compile active ones """
    if len(proc_list) > 10:
        for row in proc_list:
            cmd = 'kill -9 %s' % str(row[ID_index])
            logging.info('Executing: "%s"' % cmd)
            logging.info('Process: "%s"' % row)
            
            os.system(cmd)
    
    logging.info('%s processes killed.' % str(len(proc_list)))
    
    return 0

"""
    Call main, exit when execution is complete

""" 
if __name__ == "__main__":
    main([])
