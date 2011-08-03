
"""

    This script generates a breakdown of donation counts based on the amount donated

"""


""" Script meta """
__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "July 13th, 2011"


""" Import python base modules """
import sys, datetime, argparse, logging, operator
import pylab as plt

""" Modify the classpath to include local projects """
sys.path.append('/home/rfaulkner/trunk/projects')

""" Import Analytics modules """
import Fundraiser_Tools.classes.DataLoader as DL


 
"""
    Define script usage
"""
class Usage(Exception):
    
    def __init__(self, msg):
        self.msg = msg

"""
    Handles the 'time' argument
"""
def param_pass(input):
    
    return input

"""
    Execution body of main
"""
def main(args):
    
    countries = ['US', 'DE', 'JP', 'GB', 'FR', 'CA', 'IT', 'RU', 'BR', 'PL', 'MX', 'ES', 'IN', 'AU', 'NL', 'SE', 'CO', 'AR', 'UA', 'TW', 'CH', 'AT', 'TR', 'PH', 'BE', 'FI', 'CN', 'ID', 'NO', 'HK']

    sql = "select total_amount, count(*) as counts from civicrm.civicrm_contribution " \
    "join civicrm.civicrm_address on civicrm.civicrm_contribution.contact_id = civicrm.civicrm_address.contact_id " \
    "join civicrm.civicrm_country on civicrm.civicrm_address.country_id = civicrm.civicrm_country.id " \
    "where receive_date >=  '20101101000000' and receive_date < '20110101000000' and iso_code = '%s' group by 1 order by 1 asc"

    """ Configure the logger """
    
    LOGGING_STREAM = sys.stderr
    logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')
    
    print args.starttime
    
    """ PROCESS ARGS """    
    logging.info('Processing command line args ....')
    """ TODO FIXME -- add logic to process timestamps and execute """


    logging.info('Bulding Donation counts ...')
    logging.info('Processing countries: ' + str(countries))
    
    dl = DL.DataLoader()
    dl.init_db()
    
    for iso in countries:
        
        logging.info('... for ' + iso + ' ...')
        
        results = dl.execute_SQL(sql % iso)
        sorted_results = sorted(results, key=operator.itemgetter(0), reverse=False)
        sorted_results_lists = dl.break_results_into_lists(sorted_results)
        
        total_amount_index = sorted_results_lists[0]
        donation_counts = sorted_results_lists[1]
        
        indices = range(len(total_amount_index))
        for i in indices:
            if total_amount_index[i] > 100:
                cutoff = i
                break
        
                
        plt.plot(total_amount_index[:cutoff], donation_counts[:cutoff])
        plt.xlabel('Donated Amount')
        plt.ylabel('Donation Counts')
        
        plt.savefig('donor_breakdowns_FR2010_' + iso + '.png', type='png')
        plt.close()
        
    dl.close_db()
    logging.info('All donation counts processed.')
    
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
    
    parser.add_argument('-s', '--starttime', metavar="<input>", type=param_pass, help='The start time to be used for the query.', default=sys.stdin)
    parser.add_argument('-e', '--endtime', metavar="<input>", type=param_pass, help='The month of the log to be mined.', default=sys.stdin)
    parser.add_argument('-c', '--country', metavar="<input>", type=param_pass, help='The day of the log to be mined.', default=sys.stdin)
    
    args = parser.parse_args()

    sys.exit(main(args))
