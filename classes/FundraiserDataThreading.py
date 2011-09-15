"""

This module provides access to the threading classes for Fundraiser Analytics related tasks.  
This is configured as a set of classes that extend the python threading classes to build
containers for multi-threading where needed

"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "June 10th, 2011"

import re
from multiprocessing import Process

import classes.DataMapper as DM

"""
    This class handles executing a log mining process in a new thread
"""
class MinerThread ( object ):
        
    def __init__(self, log_name):
        
        self._fdm_ = DM.FundraiserDataMapper()
        self._log_name_ = log_name
        self._process_ = Process(target=self.call_mine_log())
        
    def run( self ):
        self._process_.start()
        # self._process_.join()
    
    
    def call_copy_log(self, type, **kwargs):
        
        return
    
    def call_mine_log(self):
        
        dm = DM.DataMapper()
        
        """ Determine whether logs are for banner impressions or landing pages """
        
        if re.search('bannerImpressions', self._log_name_):
            print 'New Thread:  Mining banner impressions from ' + self._log_name_
            
            if dm.log_exists(self._log_name_ + '.log.gz'):
                
                try:
                    self._fdm_.mine_squid_impression_requests(self._log_name_ + '.log.gz')
                
                except Exception as e:
                    print type(e)     # the exception instance
                    print e.args      # arguments stored in .args
                    print e           # __str__ allows args to printed directly
                

            elif dm.log_exists(self._log_name_ + '.log'):
                
                try:
                    self._fdm_.mine_squid_impression_requests(self._log_name_ + '.log')
                    
                except Exception as e:
                    print type(e)     # the exception instance
                    print e.args      # arguments stored in .args
                    print e           # __str__ allows args to printed directly

                
        elif re.search('landingpages', self._log_name_):
            print 'New Thread:  Mining landing page views from ' + self._log_name_
            
            if dm.log_exists(self._log_name_ + '.log.gz'):
                
                try:
                    self._fdm_.mine_squid_landing_page_requests(self._log_name_ + '.log.gz')
                
                except Exception as e:
                        print type(e)     # the exception instance
                        print e.args      # arguments stored in .args
                        print e           # __str__ allows args to printed directly
            
            elif dm.log_exists(self._log_name_ + '.log'):
                
                try:
                    self._fdm_.mine_squid_landing_page_requests(self._log_name_ + '.log')
                
                except Exception as e:
                            print type(e)     # the exception instance
                            print e.args      # arguments stored in .args
                            print e           # __str__ allows args to printed directly
        
           