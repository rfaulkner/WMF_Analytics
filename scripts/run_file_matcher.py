
"""

    Wrapper script to read a set of patterns from log files and to count each request included in the matched log files that matches another (typicall a url) pattern

"""


""" Script meta """
__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "September 29th, 2011"


""" Import python base modules """
import sys, logging, argparse

""" Import Analytics modules """
import classes.FileHandler as FH



"""
    Handles the 'time' argument
"""
def direct_input(input):    
    return input

"""
    Execution body of main
"""
def main(args):
    
    """ Configure the logger """
    LOGGING_STREAM = sys.stderr
    logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')
    
    logging.info('Looking for logs...')
               
    fh = FH.LogFileHandler()
    
    try:
        # e.g.
        # log_filename_patterns = ['landingpages-2011-09-29', 'landingpages-2011-09-28', 'landingpages-2011-09-27']
        # fh.set_proc_params(' ', [8], ['http://wikimediafoundation.org/wiki/Answers'])
        log_filename_patterns = [args.file_pattern]
        fh.set_proc_params(args.delimter, [args.index], [args.line_pattern])
        
    except:
        logging.error('')
        return -1
    
    """ Retrieve log files """
    log_files = list()
    for pattern in log_filename_patterns:        
        log_files.extend(fh.get_logfiles_from_pattern(pattern))

    """ Perform matching and count lines """
    line_matches = 0
    files_read = 0
    for log_file in log_files:
        
        logging.info('Reading %s...' % log_file)
        output = fh.read_file(log_file)
        
        files_read = files_read + 1
        line_matches = line_matches + len(output)        
        
    logging.info('Log read complete. %s lines matched over %s log files.' % (str(line_matches), str(files_read)))
    
    return line_matches, files_read


"""
    Call main, exit when execution is complete
    
    Argument parsing (argparse) and pass to main

""" 
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description='Extracts revert data in db42.wikimedia.org:halfak and db42.wikimedia.org:enwiki.'
    )
    
    """ Allow specification of the log time in CLI arguments """
    
    parser.add_argument('-d', '--delimiter', metavar="<input>", type=direct_input, help='The delimiter on which to split requests.', default=sys.stdin)
    parser.add_argument('-i', '--index', metavar="<input>", type=direct_input, help='The index of delimited requests.', default=sys.stdin)
    parser.add_argument('-f', '--file_pattern', metavar="<input>", type=direct_input, help='The filename substring matched in selected files.', default=sys.stdin)
    parser.add_argument('-l', '--line_pattern', metavar="<input>", type=direct_input, help='The pattern on which to match delimited samples.', default=sys.stdin)
    
    args = parser.parse_args()
    
    main(args)
