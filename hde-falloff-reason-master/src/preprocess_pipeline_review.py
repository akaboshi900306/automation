#!/usr/bin/env python
# coding: utf-8


"""
clean up review text,
break down to sentences
Version 2
Add: map processed sentence to raw sentences and add in outputs
"""

__version__=2.0
__author__='Haozheng Tian'


import numpy as np
import argparse
from tqdm import trange, tqdm
import os
from multiprocessing import Pool, cpu_count
import sys
import string
import re
import spacy
import itertools
import difflib
from nltk.util import ngrams
# import logging
# sys.path.append("../util")
import util, sentence_clean_filters
# logger = logging.getLogger("preprocess_pipeline_review")


################################################################################
################################################################################
################################################################################
################################################################################
## global var
# SCRIPT_KEY = "preprocess_pipeline_review"
# DATA_DIR = "./review_extraction/"
# OUTPUT_DIR = "./review_processed/"
# CATEGORY = None

NLP = spacy.load('en_core_web_sm', disable=['ner','textcat'])
NUM_REVIEW_PER_BATCH = 20000  ## a rough number to limit the number of reviews in one processed file



# sys.exit()
################################################################################
################################################################################
################################################################################
################################################################################



def formalize_str(s):
    s = remove_non_ascii(s)
    ss = s
    s = ''
    for i in ss.split():
        if i.lower() in sentence_clean_filters.contractions_dict:
            s = s+' '+sentence_clean_filters.contractions_dict[i.lower()]
        else:
            s = s+' '+i

    s = s.encode().decode("utf-8")
    return s.strip()

def remove_non_ascii(text): ## remove non ASCII chars from string
    printable = set(string.printable)
    return ''.join(filter(lambda x: x in printable, text))

def replace_URL(text): ## regex to remove URLs from strings
    return re.sub('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+','URLLINK',text)

def filter_stop_sentences(text): ## quick function to remove unwanted phrases in reviews
    for phrase in sentence_clean_filters.phraseList:
        text = text.replace(phrase,' ')
    return text

# def correct_spelling(x, spell = SPELL):
#     tmp = spell(x)
#     return(tmp)

def nlp_clean_pipe(omsid, review_key, text): ## general cleaning pipeline
    ## maybe add spelling correction

    text = formalize_str(text)
    text = replace_URL(text)
    text = filter_stop_sentences(text)
    text = text.replace('set up','setup') # should be handled in a dict like contractions
    text = text.replace('put together','assemble') # should be handled in a dict like contractions
#     text = correct_spelling(text)

    return(omsid, review_key, text)

def extractReviewTextAndID(reviews):
    #
    assert isinstance(reviews, list),"reviews should be a list."
    id_reviews = dict()

    for r in reviews:
        id_reviews[r['Id']] = r['ReviewText']

    return id_reviews


def find_best_match_sent(query_str, ls):
    ls_split = ls.split()
#     print(len(ls_split), ls_split)
    query_len = min(len(query_str.split()), len(ls_split))
    max_sim_val    = 0
    max_sim_string = u" "
#     print(val2['ReviewText'], query_length, query_str)
    for j, ngram in enumerate(ngrams(ls_split, query_len)):
        hay_ngram = u" ".join(ngram)
#         print("hay:",hay_ngram)
        similarity = difflib.SequenceMatcher(None, hay_ngram, query_str).ratio()
        if similarity > max_sim_val:
            max_idx = j
            max_sim_val = similarity
            max_sim_string = hay_ngram
    return max_sim_val


def process_review_text_pipe(file, batch_num):
    # print("QC:",DATA_DIR, OUTPUT_DIR)
    reviews = util.LoadPickledFile(os.path.join(DATA_DIR,file))
    print(f"Current batch num: {batch_num}, filename: {file}")
    ## step 1: produce reviews_dict
    reviews_dict = dict()
    for i, item in tqdm(enumerate(reviews), total = len(reviews), desc=f'Batch {batch_num}: Step1 products'):
        if item['num_reviews']>0:
            reviews_dict[item['omsid']] = dict()
            for j, item2 in enumerate(item['reviews']):
                ## store review, skip ReviewText == None
                if item2['ReviewText'] is not None:
                    reviews_dict[item['omsid']][item2['Id']] = item2
            ## remove sku with no reviews
            if len(reviews_dict[item['omsid']]) == 0:
                del reviews_dict[item['omsid']]

    ## step 2
    count = 0
    input_data = []
    for key, val in tqdm(reviews_dict.items(), total=len(reviews_dict), desc=f'Batch {batch_num}: Step2 products'):
        count += len(val)
        for review_key, review in val.items():
            input_data.append((key, review_key, review['ReviewText']))
        ## start processing and outputing
        if count >= NUM_REVIEW_PER_BATCH:
            print(f"  Exceeding NUM_REVIEW_PER_BATCH: {NUM_REVIEW_PER_BATCH}, start processing...")

            id_reviews = []
            for entry in input_data:
                id_reviews.append(nlp_clean_pipe(*entry))
            ## change add processed info
            for omsid, review_key, review in id_reviews:
                reviews_dict[omsid][review_key]['ReviewText_Cleaned'] = review
                doc = NLP(review)
                reviews_dict[omsid][review_key]['Sentences_Cleaned'] = [sent.text for sent in doc.sents]
            ## reset
            input_data = []
            count = 0
    ## step 3: take care of the rest
    len_input_data = len(input_data)
    if len_input_data>0:
        print(f"  Batch {batch_num} got left over: {len_input_data}, start processing...")
        id_reviews = []
        for entry in input_data:
            id_reviews.append(nlp_clean_pipe(*entry))
        ## change add processed info
        for omsid, review_key, review in id_reviews:
            reviews_dict[omsid][review_key]['ReviewText_Cleaned'] = review
            doc = NLP(review)
            reviews_dict[omsid][review_key]['Sentences_Cleaned'] = [sent.text for sent in doc.sents]

    ## step 4: find and add raw sentences to results
    for omsid, val in tqdm(reviews_dict.items(), total=len(reviews_dict), desc=f"Batch {batch_num}: Step4 products"):
        for review_key, val2 in val.items():
            if val2['ReviewText_Cleaned'].rstrip() != '':
                doc = NLP(val2['ReviewText'])
                sents_raw = [x.text for x in doc.sents]

                sents_matched = []
                for i, query_str in enumerate(val2['Sentences_Cleaned']):
                    sims = list(map(find_best_match_sent, itertools.repeat(query_str,len(sents_raw)), sents_raw))
                    sents_matched.append(sents_raw[np.argmax(sims)])

                reviews_dict[omsid][review_key]['Sentences_Raw'] = sents_matched
    ## step 5: output

    OUTPUT_NAME = f"HD_{CATEGORY}_review_processed_"+str(batch_num)+".pkl"
    util.DumpDataToPickle(reviews_dict, os.path.join(OUTPUT_DIR, OUTPUT_NAME))







def read_config(path):
    if not os.path.exists(path):
        print('Config file not exists. Exiting...')
        sys.exit()
    CONFIG = util.LoadJsonFile(path)
    # with open(path,'r') as f:
    #     CONFIG = json.load(f)

    return CONFIG

# In[ ]:
def main(config):
    assert isinstance(config, str),"config (str) should be a path to the config file."
    CONFIG = read_config(config)

    global DATA_DIR, OUTPUT_DIR, CATEGORY

    COMMON_INFO_KEY = "shared_info"
    SCRIPT_KEY = "preprocess_pipeline_review"
    #################################################
    #################################################
    ### make sure CONFIG has required information
    required_common_keys = ["category",
                            "input_root",
                            "dynamic_input_dir",
                            "dynamic_output_dir"
                       ]
    required_private_keys = []

    missing_keys = []
    for key in required_common_keys:
        if key not in CONFIG[COMMON_INFO_KEY]:
            missing_keys.append(key)

    for key in required_private_keys:
        if key not in CONFIG[SCRIPT_KEY]:
            missing_keys.append(key)

    if len(missing_keys) > 0:
        print("Missing keys:", "|".join(missing_keys))
        sys.exit()
    #################################################
    #################################################

    ## var0

    ## var1
    if CONFIG[COMMON_INFO_KEY]['dynamic_input_dir']:
        INPUT_ROOT = CONFIG[COMMON_INFO_KEY]['input_root']

        if not os.path.exists(INPUT_ROOT):
            print("Input root does not exist. Exiting...")
            sys.exit()

        fs = os.listdir(INPUT_ROOT)
        fs = [x for x in fs if os.path.isdir(os.path.join(INPUT_ROOT, x)) and x.endswith('_extraction') and x.startswith("20")]
        INPUT_DIR = max(fs, key = lambda x: os.path.getmtime(os.path.join(INPUT_ROOT, x)))
        DATA_DIR = os.path.join(INPUT_ROOT, INPUT_DIR)

        NOW = "_".join(INPUT_DIR.split("_")[0:2])

    else:
        DATA_DIR = "./review_extraction/"
    print("Input dir:", DATA_DIR)

    ## var2
    if CONFIG[COMMON_INFO_KEY]['dynamic_output_dir']:
        OUTPUT_DIR_CHILD = NOW+"_extraction_processed"
        OUTPUT_DIR = os.path.join(INPUT_ROOT, OUTPUT_DIR_CHILD)
    else:
        OUTPUT_DIR = "./review_processed/"
    print("Output dir:", OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    ## var3
    CATEGORY = CONFIG[COMMON_INFO_KEY]['category']

    files = os.listdir(DATA_DIR)
    ## filter files
    files = [f for f in files if f.endswith(".pkl")]
    # print(len(files))


    pool = Pool(cpu_count()-1)
    input_data = [(f, i+1) for i, f in enumerate(files)]
    pool.starmap(process_review_text_pipe, input_data)
    pool.close()
    pool.join()
    print("Done!!")

if __name__ == '__main__':
    ## parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('config',type=str, help="Specify the path of config file.")
    args = parser.parse_args()

    main(args.config)
