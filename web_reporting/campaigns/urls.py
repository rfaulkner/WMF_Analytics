
import config.settings as projSet
from django.conf.urls.defaults import *


urlpatterns = patterns('',
    # url(r'^$', 'campaigns.views.index', kwargs={'message':''}),
    (r'^$', projSet.__web_app_module__ + '.campaigns.views.index'),
    (r'^(?P<utm_campaign>[a-zA-Z0-9_]+)$', projSet.__web_app_module__ + '.campaigns.views.show_campaigns'),
)
