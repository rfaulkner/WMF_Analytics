
"""
    Script to execute caching update for long term trends
"""


""" Script meta """
__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "November 17th, 2011"


""" Import python base modules """
import sys, logging
import settings as projSet
sys.path.append(projSet.__project_home__)

""" Import Analytics modules """
import classes.SystemMonitor as SM
import classes.DataCaching as DC
import data.web_reporting_view_keys as view_keys

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

    user = projSet.__system_user__
    cmd_pattern = 'run_cache_ltt'
    
    sm = SM.SystemMonitor()
    
    if sm.check_for_process_in_PIDfile(user, cmd_pattern):
    
        try:
            ltt_dc = DC.LTT_DataCaching()
            ltt_dc.execute_process(view_keys.LTT_DICT_KEY)     
            sm.remove_process_from_PIDfile(user, cmd_pattern)
        except Exception as inst:
            logging.info('Unable to produce long term trends: %s' % str(inst))
        finally:
            sm.remove_process_from_PIDfile(user, cmd_pattern)           
    
    else:
        
        logging.info('Process for "run_cache_ltt" is already running.')
        
    return 0

"""
    Call main, exit when execution is complete

""" 
if __name__ == "__main__":
    main([])