
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
import classes.DataMapper as DM

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
    
    
    log_filename = 'bannerImpressions-2011-11-22-11PM--00.log.gz'
    
    fdm = DM.FundraiserDataMapper()
    fdm.copy_log_by_filename(log_filename)
    fdm.mine_squid_impression_requests_raw(log_filename)
    
    return 0

"""
    Call main, exit when execution is complete

""" 
if __name__ == "__main__":
    main([])