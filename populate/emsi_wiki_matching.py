#from ..preprocess.utils import *
#import preprocess.utils as pu
from populate.utils import generate_cluster_skills,evaluate_str
from populate.imports import *
from constants import URI, USER, PASSWORD



emsi = pd.read_csv('/home/ubuntu/test/repo_test/datasets/Emsi_wiki_skills_vs.csv', index_col=0)

emsi['related_skills'] = evaluate_str(emsi['related_skills'])
emsi = emsi.drop_duplicates(subset=['related_skills'])
GRAPH = Graph(f"{URI}", user=f"{USER}", password=f"{PASSWORD}", name="neo4j")



generate_cluster_skills(graph,emsi['related_skills'],'RELATES_TO_WIKI','wiki')