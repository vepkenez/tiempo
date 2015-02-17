from twisted.application import service, internet
from twisted.internet import reactor
from twisted.python.threadpool import ThreadPool
from twisted.web.server import Site

import logging

logger = logging.getLogger(__name__)


class Service(service.MultiService):
    """
    """

    def __init__(self, resource, port=3333, **kwargs):
        service.MultiService.__init__(self)

        self.addThreadPool()
        self.addTCPServer('root', resource, port)

    def addThreadPool(self, **kwargs):
        # Create, start and add a thread pool service, which is made available
        # to our WSGIResource within HendrixResource
        threads = ThreadPool()
        reactor.addSystemEventTrigger('after', 'shutdown', threads.stop)
        thread_service = ThreadPoolService(threads)
        self.addNamedService('threads', thread_service)

    def addTCPServer(self, name, resource, port):
        factory = Site(resource)
        tcp_service = TCPServer(port, factory)
        self.addNamedService(name, tcp_service)

    def addNamedService(self, name, service):
        # to get this at runtime use service.getServiceNamed('service_name')
        service.setName(name)
        service.setServiceParent(self)

    def getPort(self, name):
        "Return the port object associated to our tcp server"
        service = self.getServiceNamed(name)
        return service._port


class ThreadPoolService(service.Service):
    '''
    A simple class that defines a threadpool on init
    and provides for starting and stopping it.
    '''
    def __init__(self, pool):
        "self.pool returns the twisted.python.ThreadPool() instance."
        if not isinstance(pool, ThreadPool):
            msg = '%s must be initialised with a ThreadPool instance'
            raise TypeError(
                msg % self.__class__.__name__
            )
        self.pool = pool

    def startService(self):
        service.Service.startService(self)
        self.pool.start()

    def stopService(self):
        service.Service.stopService(self)
        self.pool.stop()


class TCPServer(internet.TCPServer):

    def __init__(self, port, factory, *args, **kwargs):
        internet.TCPServer.__init__(self, port, factory, *args, **kwargs)
        self.factory = factory
