from populate.imports import *
from constants import URI, USER, PASSWORD
from populate.utils import create_nodes_from_df
import pandas as pd

# Reading the CSV file and storing it in a pandas DataFrame
emsi_skills = pd.read_csv('Emsi_processed.csv', index_col=0)

# Dropping unnecessary columns from the DataFrame
emsi_skills = emsi_skills.drop(['Unnamed: 0', 'related_keyphrases'], axis=1)


GRAPH = Graph(f"{URI}", user=f"{USER}", password=f"{PASSWORD}", name="neo4j")


# create nodes in the graph using the DataFrame
create_nodes_from_df(graph, emsi_skills, 'Skill')




