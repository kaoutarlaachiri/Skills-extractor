import pandas as pd
from utils import search_words
from constants import EMSI_PATH
'''
Since Wikipedia API has a limit of 3000 queries for each use,
The large csv file is divided into chuncks and each chunk is then passed to the search_words 
function imported from search file, which adds related keyphrases to each skill.
Then, each chunk is saved in a related keyphrases CSV file. 
Finally, all of the chunks are concatenated into a single dataframe and saved in a new CSV file called keyphrases.csv.
'''
#define chunk size 
size = 2000

# dataframe that will contain all data
df=pd.DataFrame()

emsi=pd.read_csv(EMSI_PATH, chunksize=size)
dice = pd.read_csv('/home/ubuntu/test/repo_test/datasets/dice_exoloded.csv', chunksize=size)
# Iterating over chunks from csv file
for chunk in dice:
    
    # applying search_words function from search module on each chunk
    search_words(chunk,'skills')
    
    # Appending each chunk to a new row in the csv file '/home/ubuntu/test/skills/dataset/related_keyphrases.csv' in append mode with header turned off.
    #chunk.to_csv('/home/ubuntu/test/skills/dataset/related_keyphrases_fr.csv', mode='a', header=False)

    # Concatenating each chunk into df.
    df = pd.concat([df, chunk])
    
    # Saving all concatenated data in the df dataframe in csv 
    df.to_csv('/home/ubuntu/test/repo_test/datasets/dice_wiki.csv')

