
"""
    Script to execute caching update for live results
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
    cmd_pattern = 'run_cache_live_results'
    
    sm = SM.SystemMonitor()
   
    if sm.check_for_process_in_PIDfile(user, cmd_pattern):
        
        try:
            lr_dc = DC.LiveResults_DataCaching()
            lr_dc.execute_process(view_keys.LIVE_RESULTS_DICT_KEY)     
            sm.remove_process_from_PIDfile(user, cmd_pattern)
        except Exception as inst:
            logging.info('Unable to produce live results: %s' % str(inst))
        finally:
            sm.remove_process_from_PIDfile(user, cmd_pattern)            
    else:
        
        logging.info('Process for "run_cache_live_results" is already running.')
        
    return 0

"""
    Call main, exit when execution is complete

""" 
if __name__ == "__main__":
    main([])