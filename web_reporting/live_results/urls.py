
import config.settings as projSet
from django.conf.urls.defaults import *

urlpatterns = patterns('',
    (r'^$', projSet.__web_app_module__ + '.live_results.views.index'),
    (r'^long_term_trends$', projSet.__web_app_module__ + '.live_results.views.long_term_trends'),
    (r'^fundraiser_totals$', projSet.__web_app_module__ + '.live_results.views.fundraiser_totals'),
    (r'^fundraiser_totals/(?P<country>[a-zA-Z0-9_]{2})$', projSet.__web_app_module__ + '.live_results.views.fundraiser_totals_cntry'),
    (r'^impressions$', projSet.__web_app_module__ + '.live_results.views.impression_list'),
    (r'^daily_totals$', projSet.__web_app_module__ + '.live_results.views.daily_totals'),
)
