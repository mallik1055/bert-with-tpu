
#GCLOUD BUCKET CONFIG
CLOUD_BUCKET = {
    'name':'524-bert-bucket',
    'bert_model_path':'uncased_L-12_H-768_A-12', #from the root dir but dont add / in front 
    'raw_text_path':'input_data', #dir with text data to be processed
    'emb_path':'output_data', #dir where the output(bert embeddings) from the TPU will be stored
    
}
#GCLOUD TPU CONFIG
TPU = {
    'name':'tpu-vm', #name of the TPU. Check it under https://console.cloud.google.com/compute/tpus
    'ip':'10.240.1.2', #Internal IP of the TPU. Check it under under https://console.cloud.google.com/compute/tpus
    'bert_home_dir':'/home/mbudida/bert/' #The directory in gcp VM where you cloned https://github.com/google-research/bert
}

#Features extractor
#specify names and indexes of the columns you want to retain from the input csv to your output JSON file
EXTRACT_FEATURES = {
    'raw_text':2, #the main text to be processed
    'extra_feat':{  #other features along with the embeddings you want to retain in the output JSON file
        'message_id':0,
        'user_id':1,
        'create_at':3,
        'county':7,
    }
}


#INPUT files containing tweets/text to be processed
INPUT_FILE = {
    'in_hdfs':1, # 0 for local FS
    'path':'/hadoop_data/cm_english_filtered/part7/tweets.10pct.2016.part7.en.30/*',
    'chunk_start_index':5000000,
    'chunk_size':50000,
    'num_chunk':100,
    'chunk_filename_prefix':'tweets.10pct.2016.part7.en.30_' #prefix of the chunks being created
}

#OUTPUT files containing BERT emb
OUTPUT_FILE = {
    'in_hdfs':1, #Do you want to put it in HDFS
    'path':'/user/mbudida/cm_english_filtered/bert_output/'
}

#Status Log files for downloader and uploader
UPLOAD_STATUS_FILE = 'uploadStatus.csv'
DOWNLOAD_STATUS_FILE = 'downloadStatus.csv'
PROCESSING_STATUS_FILE='processingStatus.csv'



