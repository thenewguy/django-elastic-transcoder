from django.conf.urls import url, patterns

urlpatterns = patterns('dj_elastictranscoder.views',
    url(r'^endpoint/$', 'endpoint', name="elastic-transcoder-endpoint"),
)
