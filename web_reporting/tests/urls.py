
import config.settings as projSet
from django.conf.urls.defaults import *

urlpatterns = patterns('',
    (r'^$', projSet.__web_app_module__ + '.tests.views.index'),
    (r'^build_test$', projSet.__web_app_module__ + '.tests.views.test'),
    (r'^summaries$', projSet.__web_app_module__ + '.tests.views.test_summaries'),
    (r'^table_summary$', projSet.__web_app_module__ + '.tests.views.generate_summary'),
    (r'^report/(?P<utm_campaign>[a-zA-Z0-9_]+)$', projSet.__web_app_module__ + '.campaigns.views.show_report'),
    (r'^report/comment/(?P<utm_campaign>[a-zA-Z0-9_]+)$', projSet.__web_app_module__ + '.tests.views.add_comment'),
)
