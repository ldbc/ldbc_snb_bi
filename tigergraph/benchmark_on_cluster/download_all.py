import argparse
import time
import botocore
import paramiko
from scp import SCPClient

import boto3
from botocore.client import Config


parser = argparse.ArgumentParser(description='Download and uncompress the data on all the nodes.')
parser.add_argument('data',  type=int, choices=[100, 300, 1000, 3000, 10000, 30000], help='data scale factor.')
parser.add_argument('ip', type=str, help='either an starting ip with number of nodes "ip_start:nodes" or a file of IP list')
parser.add_argument('--thread','-t', type=int, default=1, help='number of threads for each node')
parser.add_argument('--parts','-p', type=int, default=0, help='number of parts to split the data (0 means the same as node number)')
parser.add_argument('--start','-s', type=int, default=0, help='the start index of the data partition (default 0)')
parser.add_argument('--access_key_id', type=str, default=None, help='Access Key ID')
parser.add_argument('--secret_access_key', type=str, default=None, help='Secret Access key with permission to read the bucket')
parser.add_argument('--region', type=str, default="us-east-1", help='The region the bucket is located in (cloud provider dependent)')
parser.add_argument('--bucket_name', type=str, help='The name of the bucket')
parser.add_argument('--provider', type=str, default="AWS", choices=['AWS', 'GCP'], help="Cloud provider to use (Google Cloud 'GCP', Amazon Web Services 'AWS')")
args = parser.parse_args()

user = "tigergraph"
pin = "ldbcaudit" # please change the pin here
workdir = '/home/tigergraph'
root = f'sf{args.data}/'

def get_client():
    if (args.provider == "GCP"):
        s3 = boto3.client(
            's3',
            endpoint_url='https://storage.googleapis.com',
            config=Config(signature_version='s3v4')
        )
    elif (args.provider == "AWS"):
        # Default to AWS
        s3 = boto3.client('s3')
    else:
        raise ValueError(
            "Error initializing session. Please provide valid cloud provider (Google Cloud 'GCP', Amazon Web Services 'AWS')"
        )
    return s3

def createSSHClient(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client

def main():
    print("check data accessibility")
    client = get_client()
    try:
        client.head_bucket(Bucket=args.bucket_name)
    except botocore.exceptions.ClientError:
        print(f'Given bucket {args.bucket_name} does not exist.')
    print("The bucket can be accessed")
    
    ip_list = []
    if ":" in args.ip:
        # args.ip is "start_ip:nodes" 
        start, nodes = args.ip.split(":")
        start_ip = start.split('.')
        for i in range(nodes):
            ip4 = int(start_ip[-1]) + i
            ip = start_ip[:-1] + [str(ip4)]
            ip_list.append('.'.join(ip))
    else:
        # args.ip is a file of ips
        with open(args.ip,'r') as f:
            for ip_str in f:
                if len(ip_str.split('.')) != 4: continue
                ip_list.append(ip_str.strip())
      
    for i,ip in enumerate(ip_list):
        ssh = createSSHClient(ip, 22, user, pin)
        scp = SCPClient(ssh.get_transport())
        print(f'logging to {ip}')
        scp.put('../k8s/download_one_partition.py', workdir)
        scp.put('../k8s/download_decompress.sh', workdir)
        
        stdin, stdout, stderr = ssh.exec_command(f''' 
            cd {workdir}
            . .profile
            pip3 install boto3
            export SF={args.data}
            export i={i + args.start}
            export NUM_NODES={args.parts if args.parts else len(ip_list)}
            export target=~
            export DOWNLOAD_THREAD={args.thread}
            export ACCESS_KEY_ID={args.access_key_id}
            export SECRET_ACCESS_KEY={args.secret_access_key}
            export BUCKET_REGION={args.region}
            export BUCKET_NAME={args.bucket_name}
            export CLOUD_PROVIDER={args.provider}
            nohup sh download_decompress.sh $i $target > log.download 2>&1 < /dev/null &  
        ''')
        time.sleep(4)
        stdin, stdout, stderr = ssh.exec_command(f'tail {workdir}/log.download')
        for line in stdout.read().splitlines():
            print(line.decode('utf-8'))
      
        ssh.close()
        scp.close()  
  
if __name__ == '__main__':
    main()
