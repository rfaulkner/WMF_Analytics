
import Fundraiser_Tools.settings as projSet

# This also imports the include function
from django.conf.urls.defaults import *
from django.conf import settings

from django.contrib import admin
admin.autodiscover()



urlpatterns = patterns('',
    (r'^media/(?P<path>.*)$', 'django.views.static.serve', {'document_root': settings.MEDIA_ROOT }),
    (r'^$', projSet.__web_app_module__ + '.views.index'),
    (r'^campaigns/', include(projSet.__web_app_module__ + '.campaigns.urls')),
    (r'^tests/', include(projSet.__web_app_module__ + '.tests.urls')),
    (r'^live_stats/', include(projSet.__web_app_module__ + '.live_results.urls')),
    (r'^live_lps/', include(projSet.__web_app_module__ + '.live_lps.urls')),
    (r'^LML/', include(projSet.__web_app_module__ + '.LML.urls')),
    (r'^admin/', include(admin.site.urls)),
)
