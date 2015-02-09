from .web.resource import Resource, root
from .web.shortcuts import page


class MainDashboard(Resource):

    def render_GET(self, request):
        return page(request, 'main.html', {})


root.putChild('tiempo', MainDashboard())
