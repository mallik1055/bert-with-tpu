
#GCLOUD CONFIG
CLOUD_BUCKET = {
    'name':'524-bert-bucket',
    'raw_text_path':'/input_data',
    'emb_path':'/output_data',
    
}


#Features extractor
#specify index of the column in the csv
EXTRACT_FEATURES = {
    'raw_text':2,#the main text to be processed
    'extra_feat':{
        'message_id':0,
        'user_id':1,
        'create_at':3,
        'county':7,
    }
}


#INPUT files containing tweets/text to be processed
INPUT_FILE = {
    'in_hdfs':1,
    'path':'/hadoop_data/cm_english_filtered/part7/tweets.10pct.2016.part7.en.30/*',
    'chunk_start_index':5000000,
    'chunk_size':50000,
    'num_chunk':100,
    'chunk_filename_prefix':'tweets.10pct.2016.part7.en.30_' #prefix of the chunks being created
}

#OUTPUT files containing BERT emb
OUTPUT_FILE = {
    'in_hdfs':1,
    'path':'/user/mbudida/cm_english_filtered/bert_output/'
}

#Status Log files for downloader and uploader
UPLOAD_STATUS_FILE = 'uploadStatus.csv'
DOWNLOAD_STATUS_FILE = 'downloadStatus.csv'




