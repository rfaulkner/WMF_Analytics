"""
    DJANGO VIEW DEFINITIONS:
    ========================
    
    A very basic home url view for the Fundraiser Web Reporting
    
    Views:
    
    index           -- Simply calls the index template that links to the functional pieces of web reporting
    
"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "June 20th, 2011"

from django.shortcuts import render_to_response

def index(request):
    return render_to_response('index.html')

    


