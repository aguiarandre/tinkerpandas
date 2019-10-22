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

# The pattern of name creation is:
#    - if the method is exposed to the user, use Python's PEP8 pattern (snake_case)
#    - if it is an internal method, use the JAVA-related Tinkerpop pattern (camelCase)

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
        
        self.g.addV().property('age', 29).property('name','marko').as_('a').\
               addV().property('lang', 'java').property('name','lop').as_('b').\
               addV().property('age', 32).property('name','josh').as_('c').\
               addV().property('lang', 'java').property('name','ripple').as_('d').\
               addV().property('age', 35).property('name','peter').as_('e').\
               addV().property('age', 27).property('name','vadas').as_('f').\
               addE('knows').from_('a').to('f').property('weight', 0.5).\
               addE('knows').from_('a').to('c').property('weight', 1.0).\
               addE('created').from_('a').to('b').property('weight', 0.4).\
               addE('created').from_('c').to('b').property('weight', 0.4).\
               addE('created').from_('e').to('b').property('weight', 0.2).\
               addE('created').from_('c').to('d').property('weight', 1.0).iterate()
        
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


    def addV_from_pandas(self, dataframe, src = 'src', v_properties = ['age']):
        '''
        Create a graph given a dataframe 
        '''

        self._connect()
        self.drop_vertices()

        n_properties = len(v_properties)
        comb = []
        for i, value in dataframe.iterrows():
            vertex = f'.addV("{value[src]}")'
            properties = ''.join([f'.property("{pr}", {repr(value[pr])})' for pr in v_properties])
            comb.append(''.join([vertex, properties,]))
        execution_string = 'self.g' + '\\\n'.join(comb) + '.iterate()'
        exec(execution_string, globals())

        return self.g