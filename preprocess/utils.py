from preprocess.constants import STOP_WORDS,SKILLS_TOKENIZER,STOP_WORDS_FR
from imports import *


def search_words(df,column):
    try:
        df['wiki_keyphrases']=df[column].parallel_apply(lambda x: wikipedia.search(x))
    except Exception:
        pass

    
pattern = re.compile(r'[^\w\s]')
# def noise_cleaning(column):
#     clean = column.str.lower().str.replace(pattern, '')
#     return clean
def noise_cleaning(column):
    clean = column.apply(lambda x : [re.sub(r'[^\w\s]','', s.lower()) for s in x])
    return clean
def evaluate_str_list(df_column):
    column = df_column.apply(eval)
    return column

def evaluate_str(df):
    column = df.apply(ast.literal_eval)
    return column


def tokenize_text(text):
    tokens = SKILLS_TOKENIZER.tokenize(text)
    return tokens
def tokenize_column(column_to_tokenize):
    return column_to_tokenize.progress_apply(SKILLS_TOKENIZER.tokenize)


def remove_stopwords_text(text):
    words = text.split()
    filtered_words = [word for word in words if word.casefold() not in STOP_WORDS]
    return ' '.join(filtered_words)


def convert_str_to_list(string):
    list=string.split(',')
    list.strip()
    return list
    

def remove_stopwords_lang(tokens, lang):
    if lang == 'en':
        stop_words = STOP_WORDS
    elif lang == 'fr':
        stop_words = STOP_WORDS_FR
    else:
        stop_words = set()
    return [token for token in tokens if token not in stop_words]


def tokenize_column(column_to_tokenize):
    return column_to_tokenize.apply(SKILLS_TOKENIZER.tokenize)


def clean_column(df,column_name, lang):
    df[column_name] = df[column_name].str.lower().str.replace(pattern, '',regex=True)
    df[column_name + '_tokens'] = tokenize_column(df[column_name])
    df[column_name + '_tokens'] = df[column_name + '_tokens'].apply(lambda x: remove_stopwords_lang(x, lang))
    df['final_'+column_name] = df[column_name + '_tokens'].apply(lambda x: ' '.join(x))
    df.drop(column_name + '_tokens', axis=1, inplace=True)


def emsi_cleaning(df,english_name,french_name):
    clean_column(df,english_name,'en')
    clean_column(df,french_name,'fr')
    return df
    
    
def split_and_strip(column):
    column = column.str.split(',')
    column = column.apply(lambda x: [i.strip() for i in x])
    return column
 
    
# def df_dice_cleaning(name_column):
    
#     n_column  = split_and_strip(name_column)
#     n_column =name_column.apply(lambda x: x.replace('/',','))

#     n_column =remove_stopwords_df(name_column)
#     return n_column

def dice_prep(column):
    translator = str.maketrans(string.punctuation, ' '*len(string.punctuation))
    strip_spaces = lambda x: [s.strip() for s in x]
    column = column.str.lower()
    column = column.str.replace('/', ',')
    column = column.str.split(',')
    column = column.apply(lambda x: [s.translate(translator) for s in x])
    column = column.apply(strip_spaces)
    column = column.apply(lambda x: [s for s in x if s not in STOP_WORDS])
    return column


def row_tolist(row):
    return row.tolist()


def pdl_prep(df):
    cols_to_drop = df.filter(like='count').columns
    df.drop(columns=cols_to_drop, inplace=True)
    df['skills_list']=df.apply(row_tolist,axis=1)
    cols_to_drop = df.filter(like='skill_').columns
    df.drop(columns=cols_to_drop, inplace=True)
    return df


def explode_clean(dataframe,column):
    df = dataframe.explode(column)
    df = df.drop_duplicates(subset=[column])
    df['tokens'] = tokenize_column(df[column])
    df['tokens'] = [[token for token in tokens if token not in STOP_WORDS] for tokens in df['tokens']]
    mask = (df['tokens'].apply(lambda x: len(x) > 8))
    df.drop(df[mask].index,inplace= True)
    return df

    
def process_row1(row_column,df,column_name):
    words_set = set()
    for row2 in df[column_name]:
        if bool(set(row_column) & set(row2)):
            words_set.add(' '.join(row2))         
    return words_set


def create_related_skills_fom_tokens(df1_column,df2,df2_column_name):
    column = df1_column.apply(lambda x: process_row1(x,df2,df2_column_name))
    return column
    

def process_row2(row_column,df,column_name):
    words_set = set()
    for row2 in df[column_name]:
        if row2 in set(row_column) :
            words_set.add(row2)         
    return words_set


def create_related_skills(df1_column,df2,df2_column_name):
    column = df1_column.apply(lambda x: process_row2(x,df2,df2_column_name))
    return column


def process_row3(row_column,df,column_name):
    words_set = set()
    for row2 in df[column_name]:
        if row2 == set(row_column) :
            words_set.add(row2)         
    return words_set


def create_exact_related_skills(df1_column,df2,df2_column_name):
    column = df1_column.parallel_apply(lambda x: process_row3(x,df2,df2_column_name))
    return column


def explode_clean(dataframe,column):
    df = dataframe.explode(column)
    df = df.drop_duplicates(subset=[column])
    mask = (df[column].apply(lambda x: len(x) > 8)) & (df[column].str.contains(','))
    df=df.drop(df[mask].index)
    return df
    






    
    


    
    
    
    
    
    
    

    
