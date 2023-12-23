#from ..preprocess.utils import *
#import preprocess.utils as pu
from populate.utils import generate_cluster_skills,evaluate_str
from populate.imports import *
from constants import GRAPH


pdl = pd.read_csv('Emsi_Pdl_skills_vs.csv', index_col=0)

pdl['emsi_skills'] = evaluate_str(pdl['emsi_skills'])
graph = GRAPH


generate_cluster_skills(graph,pdl['emsi_skills'],'RELATES_TO','PDL')


