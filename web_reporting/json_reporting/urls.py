
import config.settings as projSet
from django.conf.urls.defaults import *

urlpatterns = patterns('',
    (r'^cmpgn_(?P<utm_campaign>[a-zA-Z0-9_]+)$', projSet.__web_app_module__ + '.json_reporting.views.json_out'),
)
