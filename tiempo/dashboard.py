from .web.resource import Resource, root
from .web.shortcuts import page
from .conn import REDIS
from .execution import RECENT_KEY, utc_now

from datetime import timedelta, datetime

import pytz


utc = pytz.timezone('UTC')
local = pytz.timezone("America/New_York")


class MainDashboard(Resource):

    def render_GET(self, request):
        now = utc_now()
        past = now - timedelta(days=2)
        start = past.strftime('%s')
        finish = now.strftime('%s')
        recent = REDIS.zrevrangebyscore(
            RECENT_KEY, finish, start, withscores=True
        )
        out = []
        if recent:
            out = [
                {
                    'datetime': datetime.fromtimestamp(
                        timestamp).replace(tzinfo=utc).astimezone(local),
                    'taskname': task.split(':')[0],
                    'uid': task.split(':')[-1]
                } for task, timestamp in recent
            ]

        return page(request, 'main.html', {'data': out})


root.putChild('tiempo', MainDashboard())
