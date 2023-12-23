from imports import *
from utils import dice_prep,create_related_skills,create_related_skills_fom_tokens
from constants import EMSI_PROCESSED_PATH, DICE_PATH, DICE_PATH


emsi = pd.read_csv(EMSI_PROCESSED_PATH)
dice = pd.read_csv(DICE_PATH)

dice['skills'] = dice_prep(dice['skills'])
dice['emsi_skills_tokens']= create_related_skills_fom_tokens(dice['skills'],emsi,'skill_tokens')
dice['emsi_skills']=create_related_skills(dice['skills'],emsi,'final_skill')

dice.to_csv(DICE_PATH)