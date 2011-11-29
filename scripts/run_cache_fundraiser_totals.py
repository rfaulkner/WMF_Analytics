
"""
    Script to execute caching update for fundraiser totals
"""


""" Script meta """
__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "November 28th, 2011"


""" Import python base modules """
import sys, logging
import settings as projSet
sys.path.append(projSet.__project_home__)

""" Import Analytics modules """
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
    
    ft_dc = DC.Fundraiser_Totals_DataCaching()
    ft_dc.execute_process(view_keys.FR_TOT_DICT_KEY) 
    
    return 0

"""
    Call main, exit when execution is complete

""" 
if __name__ == "__main__":
    main([])