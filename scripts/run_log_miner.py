
"""

    This script will run the 

"""


""" Script meta """
__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "July 13th, 2011"


""" Import python base modules """
import sys, datetime, settings

""" Modify the classpath to include local projects """
sys.path.append(settings.__project_home__)

""" Import Analytics modules """
import Fundraiser_Tools.classes.DataMapper as DM
    
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
    
    """ Configure the logger """
    
    LOGGING_STREAM = sys.stderr
    logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')
    
    logging.info('Looking for new logs on %s:%s' % (__squid_log_server__, __squid_log_home__))
       
    fdm = DM.FundraiserDataMapper()
    fdm.poll_logs()
    
    logging.info('Log polling complete.')
    
    return 0


"""
    Call main, exit when execution is complete
    
    Argument parsing (argparse) and pass to main

""" 
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description='Extracts revert data in db42.wikimedia.org:halfak and db42.wikimedia.org:enwiki.'
    )
    
    """ No args """ 
    
    args = parser.parse_args()

    sys.exit(main(args))
