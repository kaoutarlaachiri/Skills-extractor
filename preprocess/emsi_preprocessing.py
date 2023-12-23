from imports import *
from utils import emsi_cleaning
from constants import EMSI_PATH, EMSI_PROCESSED_PATH




emsi = pd.read_csv(EMSI_PATH)
emsi_cleaning(emsi,'english_name','french_name_deepl')
emsi.to_csv(EMSI_PROCESSED_PATH)