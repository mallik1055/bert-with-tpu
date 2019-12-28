import subprocess
import pandas as pd
from datetime import datetime
from sys import exit
import csv
from os import path
import argparse

UPLOAD_STATUS_FILE = 'uploadStatus.csv'
FILE_NAME_PREFIX = 'tweets.10pct.2016.part7.en.30_'
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk_start_index", default=None, type=int, required=True, help = 'Starting index of the data')
    parser.add_argument("--chunk_size", default=None, type=int, required=True)
    parser.add_argument("--num_chunk", default=None, type=int, required=True, help = 'Number of chunks to upload')
    args = parser.parse_args()
    start_index = args.chunk_start_index
    size = args.chunk_size
    iterations = args.num_chunk
   
    checkOrCreateFile()
    upload_df = pd.read_csv(UPLOAD_STATUS_FILE)
    for itr in range(iterations):
        end_index = start_index + size
        file_name = FILE_NAME_PREFIX + str(end_index) + "_" + str(start_index) + ".csv"
        chunkCreateCmd = getCreateChunkCmd(end_index, size, file_name)
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

    cmd = "gsutil cp ./" + file_name + " gs://bert-data-trial/bert/input_data"
    return cmd

def getCreateChunkCmd(end_index, size, file_name):

    cmd = "hadoop fs -cat /hadoop_data/cm_english_filtered/part7/tweets.10pct.2016.part7.en.30/* | head -{end_idx} " \
          "| tail -{chunk} > "+ file_name
    return cmd.format(end_idx = end_index, chunk=size)

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
