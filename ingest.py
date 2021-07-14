"""
Python Ingest Script for a series of local folders

Requires Python & pyPreservica
https://www.python.org/downloads/windows/
 
$pip install pyPreservica

The credentials.properties file should be in the same directory as the ingest.py script.

Ingest Folders from a drive specfied by data.folder into a Preservica folder specfied by parent.folder

The folder names should be unique

The security tag of the ingested folders is set to the security tag of the parent

"""

from pyPreservica import *
import configparser
import os
import zipfile


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if __name__ == '__main__':
    entity = EntityAPI()
    upload = UploadAPI()

    config = configparser.ConfigParser()
    config.read('credentials.properties', encoding='utf-8')
    parent_folder_id = config['credentials']['parent.folder']

    # check parent folder exists
    parent = entity.folder(parent_folder_id)
    print(f"Packages will be ingested into {parent.title}")

    data_folder = config['credentials']['data.folder']
    print(f"Packages will be created from folders in {data_folder}")

    subfolders = [f.path for f in os.scandir(data_folder) if f.is_dir()]

    num_folders = len(subfolders)
    print(f"Found {num_folders} folders to ingest")

    group_size = int(config['credentials']['group.size'])
    bucket = config['credentials']['bucket']
    os.chdir(data_folder)

    stop_num = int(config['credentials']['stop.after'])
    num_ingests = 0

    # get a group of folders we can ingest together
    for batch in chunks(subfolders, group_size):
        batches = [os.path.basename(b) for b in batch]
        # check for already existing folder in Preservica
        for f in list(batches):
            result = entity.identifier("code", f)
            if len(result) > 0:
                print(f"Found folder {f} in Preservica, skipping...")
                batches.remove(f)
        if len(batches) > 0:
            print(f"Creating Submission Package for:")
            print(batches)
            zipfile_name = f"{batches[0]}.zip"
            zf = zipfile.ZipFile(zipfile_name, "w")
            for root_folder in batches:
                for dirname, subdirs, files in os.walk(root_folder):
                    zf.write(dirname)
                    for filename in files:
                        zf.write(os.path.join(dirname, filename))
            zf.close()
            print(f"Uploading Submission to {bucket}")
            upload.upload_zip_package_to_S3(path_to_zip_package=zipfile_name, folder=parent, 
                                            bucket_name=bucket,
                                            callback=UploadProgressCallback(zipfile_name), 
                                            delete_after_upload=True)
            num_ingests = num_ingests + 1
            if stop_num > 0:
                if stop_num >= num_ingests:
                    break

    print(f"\nUploads Complete")
