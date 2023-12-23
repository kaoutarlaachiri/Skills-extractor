import pandas as pd
from utils import dice_prep,explode_clean
from constants import DICE_PATH
import os

# dataframe that will contain all data
df=pd.DataFrame()

dice = pd.read_csv(DICE_PATH)

dice['skills'] = dice_prep(dice['skills'])
dice = explode_clean(dice,'skills')


dice.to_csv(os.path.join(DICE_PATH, 'cleaned_dice'),index=False)


