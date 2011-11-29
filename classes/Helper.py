

"""

This module effectively functions as a Singleton class.

Helper is a bucket for miscellaneous general methods that are needed and have yet to be grouped with other functionality. 

"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "May 3rd, 2011"


import logging, sys


import config.settings as projSet

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

    

class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

"""
    Given a Filename create file object, read file, and convert to a string
"""
def file_to_string(filename):

    file = open(filename, 'r')

    file_str = ''
    line = file.readline()
    while (line != ''):
        file_str = file_str +  ' ' + line
        line = file.readline()
    
    file.close()
    
    return file_str

    
"""

    Helper method that combines dictionaries for consumption by javascript in live_results/index.html
    
    @param dict_list: A list of dictionaries with identical keys 
    
    @return: a dict where the elements of each key for of the input is combined into a list of elements
    
"""
def combine_data_lists(dict_list):
    
    try:
        template_keys = dict_list[0].keys()
        num_elems = len(dict_list)
    except:
        logging.error(projSet.__web_home__ + 'live_results/views.py: No template data found.')
        return -1

    combined_dict = dict()
    
    for key in template_keys:
        key_list = list()
        for i in range(num_elems):
            key_list.append(dict_list[i][key])
        combined_dict[key] = key_list
        
    return combined_dict

