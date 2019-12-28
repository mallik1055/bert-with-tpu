import subprocess
import pandas as pd
from sys import exit
import csv
import time
from google.cloud import storage
from os import path
from datetime import datetime

BUCKET_NAME = 'bert-data-trial'
INPUT_FOLDER_NAME = 'bert/input_data'
OUTPUT_FOLDER_NAME = 'bert/output_data/'
PROCESSING_STATUS_FILE = 'processingStatus.csv'

def process_file(file_name):
    prefix = 'gs://' + BUCKET_NAME+ '/bert/uncased_L-12_H-768_A-12/'
    vocab_file = prefix + 'vocab.txt'
    config_file = prefix + 'bert_config.json'
    checkpoint_file = prefix + 'bert_model.ckpt'

    input_file = 'gs://' + BUCKET_NAME + '/' + file_name
    #file_name contains the folder path as well, so splitting it into path and the name
    name = file_name.rsplit('/', 1)[1]
    output_file = 'gs://' + BUCKET_NAME +'/' + OUTPUT_FOLDER_NAME + name.replace('.csv', '.json')

    CMD = "python /home/sania_parveen/bert/extract_features.py --input_file=%s --output_file=%s  --vocab_file=%s   --bert_config_file=%s --init_checkpoint=%s  --layers=-2   --max_seq_length=128 --use_tpu=True --tpu_name=sania-parveen --batch_size=8 --master=grpc://10.240.1.2:8470 --num_tpu_cores=8" % (input_file, output_file, vocab_file, config_file, checkpoint_file)
    
    start_time = datetime.now()
    if runCmd(CMD) != 0:
        print('Terminating execution as previous command failed')
        exit()
    duration = datetime.now() - start_time
    addFileEntry(file_name, output_file, duration)

def addFileEntry(input_file, output_file, duration):
    checkOrCreateFile()
    current_time = datetime.now()
    row = [input_file, output_file, duration, current_time]
    with open(PROCESSING_STATUS_FILE, 'a') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(row)
    csvFile.close()


def getFileNames():
    print('Fetching names of all files in folder:' + INPUT_FOLDER_NAME + ' of bucket:' + BUCKET_NAME)
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix = INPUT_FOLDER_NAME)
    #bucket = storage_client.get_bucket(BUCKET_NAME)
    blob_objs = []
    for blob in blobs:
        if str(blob.name).endswith('.csv'):
            blob_obj = {}
            blob_obj['name'] = blob.name
            blob_obj['updated'] = blob.updated
            blob_objs.append(blob_obj)
    blob_objs.sort(key= lambda x: x['updated'])
    sorted_file_names = [blob_obj['name'] for blob_obj in blob_objs]
    print('Found total ' + str(len(sorted_file_names)) + ' files in folder')
    return sorted_file_names

def getUnprocessedFile(file_names):
    df = pd.read_csv(PROCESSING_STATUS_FILE, header = 0)
    for file_name in file_names:
        #if df['InputFileName'].str.find(file_name) == -1:
        if file_name not in df['InputFileName'].values:
            return file_name
    return 'None'


def checkOrCreateFile():
    if not path.exists(PROCESSING_STATUS_FILE):
        print(PROCESSING_STATUS_FILE + " does not exist. Creating the file...")
        header = [['InputFileName', 'OutputFileName', 'ProcessingTime', 'TimeStamp']]
        with open(PROCESSING_STATUS_FILE, 'w') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(header)
        csvFile.close()

def runCmd(cmd):
    print('Executing command: ' + cmd)
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()
        if(proc.returncode == 0):
            print('Command output' + stdout.decode('ascii'))
            return 0
        else:
            print('Failed to execute command->' + str(cmd) + '. Error->' + stderr.decode('ascii') + '. Stdout-> '
                  + stdout.decode('ascii'))
    except Exception as e:
        print('Exception occurred while executing command ->' + str(cmd) + '. Exception->' +str(e))
        return -1

def main():
    checkOrCreateFile()
    while(True):
        file_names = getFileNames()
        unprocessed_file = getUnprocessedFile(file_names)
        if unprocessed_file != 'None':
            process_file(unprocessed_file)
        else:
            print('No file to process. Sleeping for 30 mins before querying the bucket again')
            time.sleep(1800)
        
if __name__ == "__main__":
    main()
