
import config.settings as projSet
from django.conf.urls.defaults import *

urlpatterns = patterns('',
    (r'^$', projSet.__web_app_module__ + '.live_results.views.index'),
    (r'^long_term_trends$', projSet.__web_app_module__ + '.live_results.views.long_term_trends'),
)
