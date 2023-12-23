from imports import *
from skills_extraction.constants import STOP_WORDS,SKILLS_SET
from tqdm import tqdm
import multiprocessing 
from io import BytesIO
from csv import writer 
import csv
import math
from Levenshtein import ratio
from nltk import ngrams
import mysql.connector
import json 
from datetime import  datetime





pattern = re.compile(r'[^\w\s]')
translator = str.maketrans(string.punctuation, ' '*len(string.punctuation))

def remove_stopwords(text):
    
    words = text.split()
    filtered_words = [word for word in words if word.casefold() not in STOP_WORDS]
    return ' '.join(filtered_words)


def cleaning(text):
    text = text.lower()
    sentences = [substring for s in text.split("\n") for substring in s.split(":")]
    sentences = [re.sub(r"\w+'", '', token) for token in sentences]
    clean_sentences = [re.sub(pattern, '', token) for token in sentences]
    clean_sentences = [s.translate(translator) for s in clean_sentences]
    # remove leading whitespace from each sentence
    clean_sentences = [re.sub(r'(^\s+|\s{2,})', ' ', s).strip() for s in clean_sentences]
    clean = [remove_stopwords(s) for s in clean_sentences if s]
    return clean

def cleaning_(text):
    text = text.lower()
    sentences = [substring for s in text.split("\n") for substring in s.split(":")]
    sentences = [re.sub(r"\w+'", '', token) for token in sentences]
    clean_sentences = [re.sub(pattern, '', token) for token in sentences]
    clean_sentences = [s.translate(translator) for s in clean_sentences]
    # remove leading whitespace from each sentence
    clean_sentences = [re.sub(r'(^\s+|\s{2,})', ' ', s).strip() for s in clean_sentences]
    clean = [remove_stopwords(s) for s in clean_sentences if s]
    clean = ' '.join(clean)
    return clean
    
def extractor(sentences):
    words_set = set()
    for sentence in sentences:
        for skill in SKILLS_SET:
            if skill in sentence:
                words_set.add(skill)
    return words_set

def extractor1(sentences):
    words_set = {skill for sentence in sentences for skill in SKILLS_SET if skill in str(sentence)}
    return words_set

def extractor_parallel(sentence):
    return {skill for skill in SKILLS_SET if skill in str(sentence)}

def extract_data_from_database_parallel(config):
    cnx = mysql.connector.connect(**config)
    mycursor = cnx.cursor()
    mycursor.execute("SELECT profil_recherche,id FROM job_offers;")
    descriptions = mycursor.fetchall()
    pool = multiprocessing.Pool()
    results = []
    for row in tqdm(descriptions, desc='Extracting skills from descriptions'):
        result = pool.apply_async(extractor_parallel, args=(row[0],))
        results.append((result, row[1]))
    pool.close()
    pool.join()
    df = pd.DataFrame(
        [(result.get(), id) for (result, id) in results],
        columns=['skills', 'id'],
        index=[id for (_, id) in results]
    )
    return df


def extract_data_from_database(config):
    cnx = mysql.connector.connect(**config)
    mycursor = cnx.cursor()
    mycursor.execute("SELECT profil_recherche,id FROM job_offers;")
    descriptions = mycursor.fetchall()
    df = pd.DataFrame(
    ((extractor1(cleaning(row[0])), row[1]) for row in tqdm(descriptions)),
    columns=['skills', 'id'],
    index=[row[1] for row in descriptions])

    return df


def extract_data_from_database_csv(config, output_file):
    cnx = mysql.connector.connect(**config)
    mycursor = cnx.cursor()

    # Use parameterized query
    sql = "SELECT profil_recherche,id FROM job_offers"
    mycursor.execute(sql)

    with open(output_file, 'w', newline='') as f:
        fieldnames = ['skills', 'id']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        while True:
            # Fetch rows in batches
            descriptions = mycursor.fetchall()
            if not descriptions:
                break

            for row in tqdm(descriptions, desc='Extracting skills from descriptions'):
                skills = extractor1(cleaning(row[0]))
                writer.writerow({'skills': skills, 'id': row[1]})
                
def extract_data_from_database_to_sql(config):
    cnx = mysql.connector.connect(**config)
    mycursor = cnx.cursor()
    sql = "SELECT profil_recherche,id FROM job_offers LIMIT 100"
    mycursor.execute(sql)
    query = ("INSERT INTO exact_matching_results "
             "(profil_recherche_id,skills, created_at) "
             "VALUES (%s, %s, %s)")


    descriptions = mycursor.fetchall()

    for row in tqdm(descriptions, desc='Extracting skills from descriptions'):
                skills = extractor1(cleaning(row[0]))
                skills_str = ', '.join(skills)
                   

                date = datetime.now().date()



                data = (row[1],skills_str, date)
                mycursor.execute(query, data)

    cnx.commit()
    mycursor.close()
    cnx.close()
            
            
def inser(results):
    skills = []
    ids = []
    for result, id in  tqdm(results, desc='insert to dataframe'):
        skills.append(result.get())
        ids.append(id)
    return pd.DataFrame({'skills': skills, 'id': ids}, index=ids)



def extract_data_from_database3(config, output_file):
    cnx = mysql.connector.connect(**config)
    mycursor = cnx.cursor()
    mycursor.execute("SELECT profil_recherche,id FROM job_offers LIMIT 100;")
    descriptions = mycursor.fetchall()
    with open(output_file, 'w', newline='') as f: 
        writer = csv.writer(f)
        writer.writerow(['skills', 'id'])
        for row in tqdm(descriptions, desc='Extracting skills from descriptions'):
            skills = extractor1(cleaning(row[0]))
            writer.writerow([skills, row[1]])
    
   
def generate_ngrams_list(sentences_list,n):
    if n == 1:
        grams = [set(sentence.split()) for sentence in sentences_list]
    else:
        grams = [set(ngrams(sentence.split(), n)) for sentence in sentences_list]
        grams = [''.join(t) for s in grams for t in s]
        
    return grams



def generate_ngrams_txt(txt):

    unigrams = txt.split()
    bigrams = [ngrams(txt.split(), 2)]
    bigrams = [' '.join(t) for s in bigrams for t in s]
    trigram = [ngrams(txt.split(), 3)]
    trigram = [' '.join(t) for s in trigram for t in s]
    return unigrams,bigrams,trigram
def word_count(skills):

    unigram = set()
    bigram = set()
    others = set()
    for skill in skills:
        words = skill.split()
        if len(words) == 1:
            unigram.add(skill)
        elif len(words) == 2:
            bigram.add(skill)
        else:
            others.add(skill)
    return unigram,bigram,others


def cosine(v1, v2):
    dot_product = sum(x*y for x, y in zip(v1, v2))
    magnitude1 = math.sqrt(sum(code * code for code in v1))
    magnitude2 = math.sqrt(sum(code * code for code in v2))
    cosine = dot_product / (magnitude1 * magnitude2)
    return cosine

def extract_words(text, skills_dict, threshold):
    results = {}
    for word in text:
        word_set = set(word.split())
        matches = ((skill, cosine([ord(c) for c in word], [ord(c) for c in skill])) for skill in skills_dict if word_set & skills_dict[skill])
        matches = ([skill for skill, dist in matches if dist >= threshold and ratio(word,skill) >= 0.8])
        if matches:
            results[word] = [skill for skill in matches]
    return results
  
def extract_uniwords(text ,skills, threshold):
    results = {}
    
    for word in text:
        word_set = set(word.split())
        matches = {skill for skill in skills if word_set & set(skill.split()) and ratio(word,skill) >= threshold  }
        if matches:
            results[word] = [skill for skill in matches]
    return results




            

def extract_from_text(text,threshold):
    cleaned = cleaning_(text)
    unigrams,bigrams,trigrams = generate_ngrams_txt(cleaned)
    unigram_emsi,bigram_emsi,others = word_count(SKILLS_SET)
    skills_dict = {skill: (set(skill.split())) for skill in bigram_emsi}

    uni_results = extract_uniwords(unigrams, unigram_emsi, 0.8)
    
    bi_results = extract_words(bigrams, skills_dict,threshold)
    return {**bi_results,**uni_results}

def extract_data_from_database3(config):
    dict = {}
    cnx = mysql.connector.connect(**config)
    mycursor = cnx.cursor()
    mycursor.execute("SELECT profil_recherche,id FROM job_offers LIMIT 100;")
    descriptions = mycursor.fetchall()
    for row in tqdm(descriptions, desc='Extracting skills from descriptions'):
            dict[row[1]] = extract_from_text((row[0]),0.8)
    
    return dict

def upload_results(config, results):
    cnx = mysql.connector.connect(**config)
    mycursor = cnx.cursor()
    date = datetime.now().date()
    query = ("INSERT INTO extraction_results "
             "(profil_recherche_id,skills, created_at) "
             "VALUES (%s, %s, %s)")

    for key, dic2 in results.items():
        skills_json = json.dumps(dic2)  # Serialize dictionary to JSON string
        data = (key,skills_json, date)
        mycursor.execute(query, data)

    cnx.commit()
    mycursor.close()
    cnx.close()

     
    

    

