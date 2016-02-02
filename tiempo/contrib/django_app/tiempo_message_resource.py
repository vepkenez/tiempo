from txsockjs.factory import SockJSResource
from tiempo.resource import TiempoMessageProtocol
from hendrix.facilities.resources import NamedResource
from twisted.internet.protocol import Factory

TiempoMessageResource = NamedResource('tiempo_communication')
TiempoMessageResource.putChild(
    'messages',
    SockJSResource(Factory.forProtocol(TiempoMessageProtocol))
)