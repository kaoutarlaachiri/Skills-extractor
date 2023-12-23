from imports import pd
from utils import pdl_prep,create_related_skills
from constants import EMSI_PATH, PDL_PATH, EMSI_PDL_PATH


emsi = pd.read_csv(EMSI_PATH)
software_eng_skills = pd.read_csv(PDL_PATH)
software_eng_skills = pdl_prep(software_eng_skills)


software_eng_skills['emsi_skills']=create_related_skills(software_eng_skills['skills_list'],emsi,'final_english_name')


software_eng_skills.to_csv(EMSI_PDL_PATH)