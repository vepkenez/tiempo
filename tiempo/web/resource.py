from twisted.web import resource, static
from twisted.web._responses import NOT_FOUND, FORBIDDEN
from .shortcuts import page


class Resource(resource.Resource):

    resourceFactory = None

    def __init__(self, *args, **kwargs):
        resource.Resource.__init__(self)
        self.args = args
        self.kwargs = kwargs

    def render(self, request):
        return resource.Resource.render(self, request)

    def getChild(self, name, request):
        if not self.resourceFactory:
            return NotFound()
        return self.resourceFactory(name, request)


class ErrorPage(Resource):

    template = '400.html'

    def __init__(self, status, brief, detail):
        Resource.__init__(self)
        self.code = status
        self.brief = brief
        self.detail = detail

    def render(self, request):
        request.setResponseCode(self.code)
        request.setHeader(b"content-type", b"text/html; charset=utf-8")
        interpolated = page(
            request, self.template, {
                'code': self.code, 'brief': self.brief, 'detail': self.detail
            }
        )
        if isinstance(interpolated, unicode):
            return interpolated.encode('utf-8')
        return interpolated

    def getChild(self, name, request):
        return self


class NotFound(ErrorPage):

    template = '404.html'

    def __init__(self, message='Sorry, this page does not exist.'):
        ErrorPage.__init__(self, NOT_FOUND, 'Page Not Found', message)


class Forbidden(ErrorPage):

    template = '403.html'

    def __init__(self, message='Sorry, this listing is forbidden'):
        ErrorPage.__init__(self, FORBIDDEN, 'Forbidden', message)


class File(static.File):
    '''
    A simple static service with directory listing disabled
    Gives the client a 403 instead of letting them browse a static directory.
    '''

    # Resource used to render 404 Not Found error pages.
    childNotFound = NotFound()

    def directoryListing(self):
        # Override to forbid directory listing
        return Forbidden()


root = Resource()
