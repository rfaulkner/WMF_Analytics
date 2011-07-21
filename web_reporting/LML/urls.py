
import Fundraiser_Tools.settings as projSet
from django.conf.urls.defaults import *


urlpatterns = patterns('',
    (r'^$', projSet.__web_app_module__ + '.LML.views.index'),
    (r'^copy_logs_form$', projSet.__web_app_module__ + '.LML.views.copy_logs_form'),
    (r'^copy_logs_process$', projSet.__web_app_module__ + '.LML.views.copy_logs_process'),
    (r'^log_list$', projSet.__web_app_module__ + '.LML.views.log_list'),
    (r'^mine_logs_process$', projSet.__web_app_module__ + '.LML.views.mine_logs_process'),
    (r'^mine_logs_process/(?P<log_name>[a-zA-Z0-9_-]+)$', projSet.__web_app_module__ + '.LML.views.mine_logs_process_file'),
    (r'^mining_patterns$', projSet.__web_app_module__ + '.LML.views.mining_patterns_view'),
    (r'^mining_patterns_add$', projSet.__web_app_module__ + '.LML.views.mining_patterns_add'),
    (r'^mining_patterns_del$', projSet.__web_app_module__ + '.LML.views.mining_patterns_delete'),
)
