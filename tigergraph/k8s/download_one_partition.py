#!/usr/bin/env python3
"""
FILE: download_one_partition.py
DESC: File containing a script to download a specific partition from
      a cloud bucket. This script is run on each node and each node
      assess what part of the partition it downloads based on the
      given index.
"""

from boto3.session import Session
from botocore.client import Config
from botocore.handlers import set_list_objects_encoding_type_url

import boto3
# boto3.set_stream_logger('')

from pathlib import Path
import argparse
from multiprocessing import Pool

# Constants
PARTITION_OR_NOT = {
    'initial_snapshot': True,
    'inserts': True,
    'deletes': False
}

STATIC_NAMES = [
    'Organisation',
    'Organisation_isLocatedIn_Place',
    'Place',
    'Place_isPartOf_Place',
    'Tag',
    'TagClass',
    'TagClass_isSubclassOf_TagClass',
    'Tag_hasType_TagClass'
]
DYNAMIC_NAMES = [
    'Comment',
    'Forum',
    'Person',
    'Post',
    'Comment_hasCreator_Person',
    'Comment_hasTag_Tag',
    'Comment_isLocatedIn_Country',
    'Comment_replyOf_Comment',
    'Comment_replyOf_Post',
    'Forum_containerOf_Post',
    'Forum_hasMember_Person',
    'Forum_hasModerator_Person',
    'Forum_hasTag_Tag',
    'Person_hasInterest_Tag',
    'Person_isLocatedIn_City',
    'Person_knows_Person',
    'Person_likes_Comment',
    'Person_likes_Post',
    'Person_studyAt_University',
    'Person_workAt_Company',
    'Post_hasCreator_Person',
    'Post_hasTag_Tag',
    'Post_isLocatedIn_Country'
]

NAMES = {
    'static':STATIC_NAMES,
    'dynamic':DYNAMIC_NAMES
}

class PartitionDownloader():
  
    def __init__(
        self,
        nodes,
        index,
        scale_factor,
        target,
        num_threads,
        cloud_provider,
        access_key_id,
        secret_access_key,
        region,
        bucket_name
    ):
        self.nodes = nodes
        self.index = index
        self.scale_factor = scale_factor
        self.target = target
        self.num_threads = num_threads
        self.cloud_provider = cloud_provider
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region = region
        self.bucket_name = bucket_name

    def create_jobs(self, session, d1_list, d2_list, is_batched):
        """
        Args:
            session (boto3.session): The session object to access
            d1_list (list(str)): Parent directories to traverse
            d2_list (list(str)): Subdirectories to traverse
            is_batched (bool): Whether the directory structure is batched
                               (e.g. update streams)
        """
        bucket = session.Bucket(self.bucket_name)
        root = f'sf{self.scale_factor}/'
        target = self.target / f'sf{self.scale_factor}'
        jobs = []
        batch = ""
        for d1 in d1_list:
            for d2 in d2_list:
                for name in NAMES[d2]:
                    loc = '/'.join([d1, d2, name]) + '/'
                    prefix = root + loc
                    if not is_batched:
                        target_dir = target / loc
                        target_dir.mkdir(parents=True, exist_ok=True)
                    i = -1
                    for blob in bucket.objects.filter(Prefix=prefix):
                        blob_name = blob.key
                        if not blob_name.endswith('.csv.gz'): continue
                        i += 1
                        if (PARTITION_OR_NOT[d1] and i % self.nodes != self.index):
                            continue

                        if is_batched:
                            batch, csv = blob_name.rsplit('/',2)[-2:]
                            target_dir = target / loc / batch
                            target_dir.mkdir(parents=True, exist_ok=True)
                        else:
                            csv = blob_name.rsplit('/',1)[-1]

                        if (name == 'Comment'):
                            print(d1, name, batch, i)

                        if self.num_threads > 1:
                            jobs.append((blob_name, target_dir/csv))
                        else:
                            session.Object(self.bucket_name, blob_name).download_file(target_dir/csv)

        return jobs

    def get_session(self):
        session = Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region
        )

        if (self.cloud_provider == "GCP"):
            session.events.unregister(
                'before-parameter-build.s3.ListObjects',
                set_list_objects_encoding_type_url
            )

            s3 = session.resource(
                's3',
                endpoint_url='https://storage.googleapis.com',
                config=Config(signature_version='s3v4')
            )
        elif (self.cloud_provider == "AWS"):
            # Default to AWS
            s3 = session.resource('s3')
        else:
            raise ValueError(
                "Error initializing session. Please provide valid cloud provider (Google Cloud 'GCP', Amazon Web Services 'AWS')"
            )
        return s3

    def download(self, jobs):
        session = self.get_session()
        for job in jobs:
            blob_name, target = job
            session.Object(self.bucket_name, blob_name).download_file(target)

    def run(self):
        session = self.get_session()

        jobs1 = self.create_jobs(
            session=session,
            d1_list=['initial_snapshot'],
            d2_list=['static', 'dynamic'],
            is_batched=False
        )
        jobs2 = self.create_jobs(
            session=session,
            d1_list=['inserts', 'deletes'],
            d2_list=['dynamic'],
            is_batched=True
        )
        
        jobs = jobs1 + jobs2

        if self.num_threads > 1:
            print(f'start downloading {len(jobs)} files ...')
            njobs = self.num_threads * 5
            jobs2 = [[] for _ in range(njobs)]
            for i, job in enumerate(jobs):
                jobs2[i % njobs].append(job)
            with Pool(processes=self.num_threads) as pool:
                pool.map(self.download, jobs2)
            print("downloading is done")

        # download parameters
        print("download parameters")
        session = self.get_session()
        bucket = session.Bucket(self.bucket_name)
        for blob in bucket.objects.filter(Prefix=f'parameters-sf{self.scale_factor}/'):
            if blob.key.endswith("/"):
                continue
            file_split = blob.key.split("/")
            directory = "/".join(file_split[0:-1])
            Path(directory).mkdir(parents=True, exist_ok=True)
            session.Object(self.bucket_name, blob.key).download_file(blob.key)
        print("download parameters done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download one partition of data from cloud storage bucket.')
    parser.add_argument('--scale_factor',  type=int, choices=[100, 300, 1000, 3000, 10000, 30000], help='Scale factor')
    parser.add_argument('--index', type=int, help='index of the node')
    parser.add_argument('--nodes', type=int, help='the total number of nodes')
    parser.add_argument('--thread','-t', type=int, default=4, help='number of threads')
    parser.add_argument('--access_key_id', type=str, default=None, help='Access Key ID')
    parser.add_argument('--secret_access_key', type=str, default=None, help='Secret Access key with permission to read the bucket')
    parser.add_argument('--target', type=Path, default=Path('~/tigergraph/data').expanduser(), help='target diretory to download')
    parser.add_argument('--region', type=str, default="us-east-1", help='The region the bucket is located in (cloud provider dependent)')
    parser.add_argument('--bucket_name', type=str, help='The name of the bucket')
    parser.add_argument('--provider', type=str, default="AWS", choices=['AWS', 'GCP'], help="Cloud provider to use (Google Cloud 'GCP', Amazon Web Services 'AWS')")
    args = parser.parse_args()

    PD = PartitionDownloader(
        nodes = args.nodes,
        index = args.index,
        scale_factor = args.scale_factor,
        target = args.target,
        num_threads = args.thread,
        cloud_provider = args.provider,
        access_key_id = args.access_key_id,
        secret_access_key = args.secret_access_key,
        region = args.region,
        bucket_name = args.bucket_name
    )

    PD.run()
