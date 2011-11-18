
"""
    Script to find and kill outstanding queries in a batch
"""


""" Script meta """
__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "November 17th, 2011"


""" Import python base modules """
import sys, MySQLdb, logging
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
    
    db = MySQLdb.connect(host=projSet.__db_server__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__, passwd=projSet.__pass__)
    cur = db.cursor()
    
    """ """
    cur.execute('SELECT * FROM INFORMATION_SCHEMA.PROCESSLIST')    
    results = cur.fetchall()    

    id_idx = 0
    user_idx = 1
    cmd_idx = 4
    
    """ Kill all sleeping threads and compile active ones """
    active_threads = list()
    for row in results:
        if cmp(row[user_idx], 'rfaulk') == 0 and not(cmp(row[cmd_idx], 'Sleep') == 0):
            active_threads.append(row)
        elif cmp(row[user_idx], 'rfaulk') == 0 and cmp(row[cmd_idx], 'Sleep') == 0:
            try:
                cur.execute('kill %s;' % str(row[id_idx]))
            except:
                logging.error('Could not kill thread %s.' % str(row[id_idx]))
                pass
            
    """ If there are more than 50 active threads terminate them all """
    if len(active_threads) > 50:
        for row in active_threads:
            try:
                cur.execute('kill %s;' % str(row[id_idx]))
            except:
                logging.error('Could not kill thread %s.' % str(row[id_idx]))
                pass
    
    return 0
