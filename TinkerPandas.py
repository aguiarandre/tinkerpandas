import logging

from gremlin_python import statics
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T
from gremlin_python.process.traversal import Order
from gremlin_python.process.traversal import Cardinality
from gremlin_python.process.traversal import Column
from gremlin_python.process.traversal import Direction
from gremlin_python.process.traversal import Operator
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Pop
from gremlin_python.process.traversal import Scope
from gremlin_python.process.traversal import Barrier
from gremlin_python.process.traversal import Bindings
from gremlin_python.process.traversal import WithOptions

statics.load_statics(globals())

class TinkerFactory(object):
        
    def __init__(self, graph = None, port = 8182):
        self.g = graph 
        self.port = port
        self.logger = logging.getLogger('TinkerFactory')
        self.logger.setLevel(level='INFO')
        
    def _connect(self):
        '''
        Connect to an instance of a gremlin server hosted at port:8182
        '''
        self.g = traversal().withRemote(DriverRemoteConnection(f'ws://localhost:{self.port}/gremlin','g'))
        
        return
    
    # The pattern of name creation is:
    #    - if the method is exposed to the user, use Python's PEP8 pattern (snake_case)
    #    - if it is an internal method, use the JAVA-related Tinkerpop pattern (camelCase)
    
    def createModern(self, drop = True):
        '''
        Generate an example graph.
        
        This method will try to connect to a localhost at a 
        specified port (default:8182) and return a graph at self.g. 
        This structure will be populated with the TinkerPop Modern example graph.
        
        '''
        self._connect()
        
        
        if drop:
            self.drop_vertices()
        
        self.g.addV().property('name','marko').as_('a').\
               addV().property('name','lop').as_('b').\
               addV().property('name','josh').as_('c').\
               addV().property('name','ripple').as_('d').\
               addV().property('name','peter').as_('e').\
               addV().property('name','vadas').as_('f').\
               addE('knows').from_('a').to('f').\
               addE('knows').from_('a').to('c').\
               addE('created').from_('a').to('b').\
               addE('created').from_('c').to('b').\
               addE('created').from_('e').to('b').\
               addE('created').from_('c').to('d').iterate()
        
        return self.g 

    def create_modern(self):
        return self.createModern()

    def drop_vertices(self):
        ''' 
        Drop all vertices before creating a new graph
        '''
        
        try:
            self.logger.info('Dropping all vertices')
            self.g.V().drop().iterate()
        except:
            self.logger.warning('Could not drop vetices of graph g')