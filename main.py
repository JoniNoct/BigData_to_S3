import os
import sys
import shutil
import zipfile
import boto3
import logging
from botocore.exceptions import ClientError
from os.path import isfile, join

ACCESS_KEY = sys.argv[1]
SECRET_KEY = sys.argv[2]

def upload_to_awsS3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def bigdata_segmentation(source, dest_folder, dest_file, subject_amount=20000):
    """Splits a large file into smaller files

    :param source: File to split
    :param dest_folder: Folder for saving split files
    :param dest_file: Name of split files
    :param subject_amount: Number of objects in each segment
    :return: None
    """

    # Initializing variables
    upper_rows = []
    content    = []
    counter    = 0
    index      = 1
    if not os.path.exists('Output/Temp/' + dest_folder):
        os.mkdir('Output/Temp/' + dest_folder)


    # Basic file splitting work
    with open(source,"r", encoding='cp1252') as file:
        upper_rows.append(file.readline())
        upper_rows.append(file.readline())
        upper_rows = "".join(upper_rows)

        for line in file:
            content.append(line)
            if line.find("</SUBJECT>"):
                counter+=1
            if counter == subject_amount:
                with open("Output/Temp/" + dest_folder + "/" + dest_file + "_part_"+str(index)+".xml", "w") as output:
                    output.write(upper_rows+"".join(content)+"</DATA>")
                    output.close()
                content = []
                counter = 0
                index  += 1
        with open("Output/Temp/" + dest_folder + "/" + dest_file + "_part_"+str(index)+".xml", "w") as output:
            output.write(upper_rows + "".join(content) + "</DATA>")
            output.close()
            content = []

# Creating temporary folders
os.mkdir("Output/Temp")
os.mkdir("Resources/Temp")

# Unzip the basic data
bigdata_zip = zipfile.ZipFile("Resources/BigData_raw.zip")
bigdata_zip.extractall("Resources/Temp")
bigdata_zip.close()

# Divide the data into segments of 20,000 units
full_paths = []
directories = os.listdir(r"Resources/Temp")
for directory in os.listdir(r"Resources/Temp"):
    full_paths = full_paths + [("Resources/Temp/" + directory + "/" + f) for f in os.listdir("Resources/Temp/" + directory) if isfile(join("Resources/Temp/" + directory, f))]
for full_path in full_paths:
    bigdata_segmentation(full_path, full_path.split("/")[3][5:-15], full_path.split("/")[3][:-4])

# Delete the temporary folder with the main data
shutil.rmtree("Resources/Temp")

# Archiving the processed data
shutil.make_archive("Output/Archives/BigData", 'zip', "Output/Temp")

# Delete the temporary folder with the processed data
shutil.rmtree("Output/Temp")

# Upload the resulting archive to AWS S3
uploaded = upload_to_awsS3("Output/Archives/BigData.zip", 'bigdata-to-s3')

# Delete the resulting archive
os.remove("Output/Archives/BigData.zip")