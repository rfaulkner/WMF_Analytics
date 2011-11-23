
import config.settings as projSet
from django.conf.urls.defaults import *

urlpatterns = patterns('',
    (r'^$', projSet.__web_app_module__ + '.live_results.views.index'),
    (r'^long_term_trends$', projSet.__web_app_module__ + '.live_results.views.long_term_trends'),
    (r'^impressions$', projSet.__web_app_module__ + '.live_results.views.impression_list'),
    (r'^(?P<utm_campaign>[a-zA-Z0-9_]+)/(?P<banner>[a-zA-Z0-9_]+)$', projSet.__web_app_module__ + '.live_results.views.json_out'),
)
