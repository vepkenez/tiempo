from django.conf.urls import include, url

urlpatterns = [
    url(r'^tiempo/', include('tiempo.contrib.django.urls')),
    ]