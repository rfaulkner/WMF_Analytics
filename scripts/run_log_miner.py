
"""

    Wrapper script will be run periodically to copy and mine udp2log files  

"""


""" Script meta """
__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "July 13th, 2011"


""" Import python base modules """
import sys, argparse, logging
import settings as projSet
sys.path.append(projSet.__project_home__)

""" Import Analytics modules """
import classes.DataMapper as DM



"""
    Define script usage
"""
class Usage(Exception):
    
    def __init__(self, msg):
        self.msg = msg

"""
    Handles the 'time' argument
"""
def time_field(input):
    
    return input

"""
    Execution body of main
"""
def main(args):
    
    """ Configure the logger """
    
    LOGGING_STREAM = sys.stderr
    logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')
    
    logging.info('Looking for new logs on %s:%s' % (projSet.__squid_log_server__, projSet.__squid_log_home__))
       
    fdm = DM.FundraiserDataMapper()
    
    """ Only process command line args if they are ALL specified """
    if isinstance(args.year, str) and isinstance(args.month, str) and isinstance(args.day, str) and isinstance(args.hour, str):
        
        logging.info('Processing command line args ....')
        fdm.poll_logs(year=args.year, month=args.month, day=args.day, hour=args.hour)
    
    else:

        logging.info('No args command line args provided. Proceeding ...')
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
    
    """ Allow specification of the log time in CLI arguments """
    
    parser.add_argument('-y', '--year', metavar="<input>", type=time_field, help='The year of the log to be mined.', default=sys.stdin)
    parser.add_argument('-m', '--month', metavar="<input>", type=time_field, help='The month of the log to be mined.', default=sys.stdin)
    parser.add_argument('-d', '--day', metavar="<input>", type=time_field, help='The day of the log to be mined.', default=sys.stdin)
    parser.add_argument('-u', '--hour', metavar="<input>", type=time_field, help='The hour of the log to be mined.', default=sys.stdin)
    
    args = parser.parse_args()

    main(args)
