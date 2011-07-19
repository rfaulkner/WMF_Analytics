from django.conf.urls.defaults import *


urlpatterns = patterns('',
    (r'^$', 'LML.views.index'),
    (r'^copy_logs_form$', 'LML.views.copy_logs_form'),
    (r'^copy_logs_process$', 'LML.views.copy_logs_process'),
    (r'^log_list$', 'LML.views.log_list'),
    (r'^mine_logs_process$', 'LML.views.mine_logs_process'),
    (r'^mine_logs_process/(?P<log_name>[a-zA-Z0-9_-]+)$', 'LML.views.mine_logs_process_file'),
    (r'^mining_patterns$', 'LML.views.mining_patterns_view'),
    (r'^mining_patterns_add$', 'LML.views.mining_patterns_add'),
    (r'^mining_patterns_del$', 'LML.views.mining_patterns_delete'),
)
