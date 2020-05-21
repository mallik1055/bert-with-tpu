# Imports the Google Cloud client library
from google.cloud import storage
from os import path
from datetime import datetime
from sys import exit
import csv
import subprocess
import time
from config_template import * #config variables


def getFilePaths():
    print('Fetching names of all files in folder:' + CLOUD_BUCKET['emb_path'] + ' of bucket:' + CLOUD_BUCKET['name'])
    storage_client = storage.Client()
    prefix_path = CLOUD_BUCKET['emb_path'] if CLOUD_BUCKET['emb_path'][0] != '/' else CLOUD_BUCKET['emb_path'][1:]
    blobs = storage_client.list_blobs(CLOUD_BUCKET['name'], prefix = prefix_path)
    #bucket = storage_client.get_bucket(BUCKET_NAME)
    blob_objs = []
    for blob in blobs:
        if str(blob.name).endswith('.json'):
            blob_obj = {}
            blob_obj['name'] = blob.name
            blob_obj['updated'] = blob.updated
            blob_objs.append(blob_obj)
    blob_objs.sort(key= lambda x: x['updated'])
    sorted_file_paths = [blob_obj['name'] for blob_obj in blob_objs]
    print('Found total ' + str(len(sorted_file_paths)) + ' files in folder')
    return sorted_file_paths

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


def checkOrCreateFile():
    if not path.exists(DOWNLOAD_STATUS_FILE):
        print(DOWNLOAD_STATUS_FILE + " does not exist. Creating the file...")
        header = [['FileName', 'Source', 'Destination', 'TimeStamp']]
        with open(DOWNLOAD_STATUS_FILE, 'w') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(header)
        csvFile.close()

def addFileEntry(file_name, source, destination):
    current_time = datetime.now()
    row = [file_name, source, destination, current_time]
    with open(DOWNLOAD_STATUS_FILE, 'a') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(row)
    csvFile.close()


def downloadAndDelFile(file_path):
    file_name = file_path.split("/")[-1]
    destination = path.join(OUTPUT_FILE['path'],file_name)
    print('Downloading file... ' + file_name)
    if OUTPUT_FILE['in_hdfs']:
        CMD = "gsutil cat gs://" + CLOUD_BUCKET['name'] + "/" + file_path + " | hadoop fs -put - " + destination
    else:
        CMD = "gsutil cat gs://" + CLOUD_BUCKET['name'] + "/" + file_path + " > " + destination

    if runCmd(CMD) != 0:
        print('Terminating execution as previous command failed')
        exit()
    # Delete copied file from bucket
    print('Deleting copied file ' + file_name)
    CMD  = "gsutil rm gs://" + CLOUD_BUCKET['name'] + "/" + file_path
    if runCmd(CMD) != 0:
        print('Terminating execution as previous command failed')
        exit()
    addFileEntry(file_name, file_path, destination)

def main():
    checkOrCreateFile()
    while(True):
        file_paths = getFilePaths()
        for i in range(len(file_paths)):
            downloadAndDelFile(file_paths[i])
        print('Sleeping for 30 mins before querying the bucket again')
        time.sleep(1800)


if __name__ == "__main__":
    main()



