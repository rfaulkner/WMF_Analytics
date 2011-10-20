
import config.settings as projSet
from django.conf.urls.defaults import *


urlpatterns = patterns('',
    (r'^$', projSet.__web_app_module__ + '.LML.views.index'),
    (r'^mining_patterns$', projSet.__web_app_module__ + '.LML.views.mining_patterns_view'),
    (r'^mining_patterns_add$', projSet.__web_app_module__ + '.LML.views.mining_patterns_add'),
    (r'^mining_patterns_del$', projSet.__web_app_module__ + '.LML.views.mining_patterns_delete'),
)
