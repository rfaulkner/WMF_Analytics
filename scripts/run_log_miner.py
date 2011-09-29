
"""

    Wrapper script will be run periodically to copy and mine udp2log files  

"""


""" Script meta """
__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "July 13th, 2011"


""" Import python base modules """
import sys, argparse, logging

""" Import Analytics modules """
import classes.DataMapper as DM
import config.settings as projSet


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
    
    if args.year or args.month or args.day or args.hour:
        
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
