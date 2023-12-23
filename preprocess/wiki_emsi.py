from imports import *
from utils import evaluate_str,create_related_skills,noise_cleaning,create_related_skills_fom_tokens
from constants import EMSI_PROCESSED_PATH, EMSI_WIKI_PATH



emsi = pd.read_csv(EMSI_PROCESSED_PATH)

emsi['related_keyphrases'] = evaluate_str(emsi['related_keyphrases'])
emsi['related_keyphrases'] = noise_cleaning(emsi['related_keyphrases'])
emsi['related_skills_tokens']= create_related_skills_fom_tokens(emsi['related_keyphrases'],emsi,'skill_tokens')
#dice['emsi_skills']=create_related_skills(dice['skills'],emsi,'final_skill')
emsi['related_skills']=create_related_skills(emsi['related_keyphrases'],emsi,'final_skill')
emsi = emsi.drop_duplicates(subset=['related_skills'])

emsi.to_csv(EMSI_WIKI_PATH)