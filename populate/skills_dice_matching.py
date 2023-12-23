#from ..preprocess.utils import *
#import preprocess.utils as pu
from populate.utils import generate_cluster_skills,evaluate_str
from populate.imports import *
from constants import GRAPH


dice = pd.read_csv('Emsi_Dice_skills.csv', index_col=0)

dice['emsi_fullskill'] = evaluate_str(dice['emsi_fullskill'])
graph = GRAPH


generate_cluster_skills(graph,dice['emsi_fullskill'],'RELATES_TO','dice')
