from django.conf.urls import include, url

from tiempo.contrib.django.views import TiempoKiosk

urlpatterns = [
    url(r'^tiempo/', include('tiempo.contrib.django.urls')),
    url(r'^tiempo_kiosk', TiempoKiosk.as_view()),
    ]