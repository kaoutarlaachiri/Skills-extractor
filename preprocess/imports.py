import pandas as pd
import re
import ast
import nltk
from nltk.tokenize import RegexpTokenizer
from tqdm import tqdm
tqdm.pandas()

from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)
nltk.download('stopwords')
import string
from nltk.corpus import stopwords
import wikipedia



