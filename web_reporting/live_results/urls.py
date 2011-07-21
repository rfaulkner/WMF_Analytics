
import Fundraiser_Tools.settings as projSet
from django.conf.urls.defaults import *

urlpatterns = patterns('',
    (r'^$', projSet.__web_app_module__ + '.live_results.views.index'),
)
