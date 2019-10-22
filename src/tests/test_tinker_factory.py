import unittest
from tinkerpandas.TinkerPandas import TinkerFactory
from hypothesis import given, strategies as st
from hypothesis.extra.pandas import data_frames, column, columns

class TestTinkerFactory(unittest.TestCase):

	def test_create_modern(self):
		g = TinkerFactory().create_modern()

	def test_modern_count_vertices(self):
		g = TinkerFactory().create_modern()
		assert g.V().count().next() == 6

	def test_pandas_vertex_creation_noproperty(self):
		dataframe = data_frames(
                columns=[column(name='src', 
                                elements=st.sampled_from(names), 
                                unique=True),
                         column(name='age', 
                                elements=st.integers(min_value=20, max_value=30), 
                                unique=False),
                       ]
            ).example()

		g = TinkerFactory().addV_from_pandas(dataframe, 
											 src='src', 
											 v_properties = [])



	def test_pandas_vertex_creation_noproperty(self):
		names = ['andre','renan','diego','caio','victor','bruno']
		languages = ['python','R','java']

		dataframe = data_frames(
                columns=[column(name='src', 
                                elements=st.sampled_from(names), 
                                unique=True),
                         column(name='age', 
                                elements=st.integers(min_value=20, max_value=30), 
                                unique=False),
                         column(name='lang', 
                                elements=st.sampled_from(languages), 
                                unique=False)
                       ]
            ).example()

		g = TinkerFactory().addV_from_pandas(dataframe, src='src', v_properties = ['age'])



