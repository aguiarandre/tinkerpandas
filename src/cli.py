from tinkerpandas.TinkerPandas import TinkerFactory

def cli():
	g = TinkerFactory().create_modern()
	print(g.V().count().next())

if __name__ == '__main__':
	cli()