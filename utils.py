from nltk import ngrams
import re
import string
import math
import json
from skills_extraction.constants import STOP_WORDS,SKILLS_SET
from tqdm import tqdm
from Levenshtein import ratio
import mysql.connector
from mysql.connector import errorcode
from nltk.corpus import stopwords
from datetime import date, datetime, timedelta
 
pattern = re.compile(r'[^\w\s]')
translator = str.maketrans(string.punctuation, ' '*len(string.punctuation))

def remove_stopwords(text):
    words = text.split()
    filtered_words = [word for word in words if word.casefold() not in STOP_WORDS]
    return ' '.join(filtered_words)

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

def extract_words(text, skills_dict, threshold=0.7):
    results = {}
    for word in text:
        word_set = set(word.split())
        print("word_set",word_set)
        matches = ((skill, cosine([ord(c) for c in word], [ord(c) for c in skill])) for skill in skills_dict if word_set & skills_dict[skill])
        matches = ([skill for skill, dist in matches if dist >= threshold and ratio(word,skill) >= threshold])
        print( "Bigram:", matches )
        if matches:
            results[word] = [skill for skill in matches]
    return results
  
def extract_uniwords(text ,skills, threshold=0.8):
    results = {}
    for word in text:

        word_set = set(word)
        print("word_set",word_set)
        matches = {skill for skill in skills if word_set & set(skill) and ratio(word,skill) >= threshold  }
        print("unigram:",matches)
        if matches:
            results[word] = [skill for skill in matches]
    return results
 

def extract_from_text(text,threshold=0.8):
    cleaned = cleaning_(text)
    unigrams,bigrams,trigrams = generate_ngrams_txt(cleaned)
    print("text 1",unigrams)
    unigram_emsi,bigram_emsi,others = word_count(SKILLS_SET)
    # print( "text emsi 1", unigram_emsi )
    skills_dict = {skill: (set(skill.split())) for skill in bigram_emsi}

    uni_results = extract_uniwords(unigrams, unigram_emsi,threshold)
    
    bi_results = extract_words(bigrams, skills_dict,threshold)
    return {**bi_results,**uni_results}
     
    
    
    
def extract_skills_from_database_dist(config):

    dict = {}
    cnx = mysql.connector.connect(**config)
    mycursor = cnx.cursor()
    mycursor.execute("SELECT profil_recherche,id FROM job_offers LIMIT 1;")
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
        skills_json = json.dumps(dic2) 
        data = (key,skills_json, date)
        mycursor.execute(query, data)

    cnx.commit()
    mycursor.close()
    cnx.close()

