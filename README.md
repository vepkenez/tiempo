# tiempo
A Procrastination Framework for Python


## Dev Setup

```bash
virtualenv env --no-site-packages --distributed
source env/bin/activate
pip install -e <path to your tiempo repo>
```

Then to test this shit out just run:

```bash
python demo/run_demo.py
```
Point your browser to 127.0.0.1:4050/tiempo_kiosk

#### To use with Django/Hendrix

*  in settings.py
  * `TIEMPO_THREAD_CONFIG = [('priority1', 'downstairs'), ('priority1, 'downstairs', 'upstairs')]`
    * each entry in the list specifies which queues that thread should check for work 
    * the above example will have two both checking "priority1" and "downstairs" and "upstairs" on only one process

  * 'INSTALLED_APPS' += 'tiempo.contrib.django_app'

  * HENDRIX_CHILD_RESOURCES += 'tiempo.contrib.django_app.tiempo_message_resource'
  
  * in urls.py
    *  add "url(r'^tiempo/', include('tiempo.contrib.django_app.urls', namespace='tiempo'))," to your patterns.
  
