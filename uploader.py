#Generates INPUT_FILE['num_chunk'] chunks of size = INPUT_FILE['chunk_size'] from INPUT_FILE['path'] and pushes to Gcloud bucket at CLOUD_BUCKET_NAME
#logs the uploaded metadata to UPLOAD_STATUS_FILE 

import subprocess
import pandas as pd
from datetime import datetime
from sys import exit
import csv
from os import path
import argparse
from config_template import * #config variables


def main():
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk_start_index", default=None, type=int, required=True, help = 'Starting index of the data')
    parser.add_argument("--chunk_size", default=None, type=int, required=True)
    parser.add_argument("--num_chunk", default=None, type=int, required=True, help = 'Number of chunks to upload')
    args = parser.parse_args()
    start_index = args.chunk_start_index
    size = args.chunk_size
    iterations = args.num_chunk
    '''
    start_index = INPUT_FILE['chunk_start_index']
    size = INPUT_FILE['chunk_size']
    iterations = INPUT_FILE['num_chunk']
    
    checkOrCreateFile()
    upload_df = pd.read_csv(UPLOAD_STATUS_FILE)
    for itr in range(iterations):
        end_index = start_index + size
        file_name = INPUT_FILE['chunk_filename_prefix'] + str(end_index) + "_" + str(start_index) + ".csv"
        chunkCreateCmd = getCreateChunkCmd(end_index, size, file_name)
        print(chunkCreateCmd)
        if runCmd(chunkCreateCmd) != 0:
            print('Terminating execution as previous command failed')
            exit()
        print('Uploading file ...' + file_name)
        uploadCmd = getUploadCmd(file_name)
        if runCmd(uploadCmd) != 0:
            print('Terminating execution as previous command failed')
            exit()
        deleteCmd = getDeleteCmd(file_name)
        print('Deleting uploaded file ...' + file_name)
        runCmd(deleteCmd)
        addFileEntry(file_name, start_index, end_index,size)
        start_index = end_index


def getDeleteCmd(file_name):
    cmd = "rm ./" + file_name
    return cmd

def getUploadCmd(file_name):

    cmd = "gsutil cp ./" + file_name + " gs://"+CLOUD_BUCKET['name'] + CLOUD_BUCKET['raw_text_path']
    return cmd

def getCreateChunkCmd(end_index, size, file_name):

    cmd = ("hadoop fs -" if INPUT_FILE['in_hdfs'] else "") + "cat "+INPUT_FILE['path']+" | head -{end_idx} | tail -{chunk} > "+ file_name
    return cmd.format(end_idx = end_index, chunk = size)

def addFileEntry(file_name, start_index, end_index, size):
    checkOrCreateFile()
    current_time = datetime.now()
    row = [file_name, start_index, end_index, size, current_time]
    with open(UPLOAD_STATUS_FILE, 'a') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(row)
    csvFile.close()

def checkOrCreateFile():
    if not path.exists(UPLOAD_STATUS_FILE):
        print(UPLOAD_STATUS_FILE + " does not exist. Creating the file...")
        header = [['FileName', 'ChunkStart', 'ChunkEnd', 'ChunkSize', 'TimeStamp']]
        with open(UPLOAD_STATUS_FILE, 'w') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(header)
        csvFile.close()

def runCmd(cmd):
    try:
        proc = subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if(proc.returncode == 0):
            return 0
        else:
            print('Failed to execute command->' + str(cmd) + '. Error->' + stderr.decode('ascii') + '. Stdout-> '
                  + stderr.decode('ascii'))
    except Exception as e:
        print('Exception occurred while executing command ->' + str(cmd) + '. Exception->' +str(e))
        return -1


if __name__ == "__main__":
    main()
