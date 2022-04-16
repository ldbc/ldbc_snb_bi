# TigerGraph implementation on cluster using K8S
## Overview
The benchmark for SF-1k and larger are performed in cluster mode and impletemented using (kubernetes)[https://kubernetes.io] (K8S). The cluster can be easily created on Google Kubernetes Engine (GKE) on Google Cloud Platform (GCP),  or Amazon Elastic Kubernetes Service (EKS) on Amazon Web Service (AWS). 

Pre-requisites on local destop
* `kubectl`
* command line tool for GCP or AWS: `gcloud` or `aws-cli`. 

## Create the cluster
On GKE, [create a container cluster](https://cloud.google.com/sdk/gcloud/reference/container/clusters/create) specifying machine type, number of nodes, disk size and disk type. For example, the following create a 2-node cluster, 
```bash
gcloud container clusters create snb-bi-tg --machine-type n2-highmem-32 --num-nodes=2 --disk-size 300 --disk-type=pd-ssd
```
On EKS, 
```bash
eksctl create cluster --name test --region us-east-2 --nodegroup-name tgtest --node-type r5.xlarge --nodes 2 --instance-prefix tg --instance-name eks-test 
```

## deploy the containers
First, deply the containers on the cluter using the script `tg` from `tigergraph/ecosys` repo. The cpu and memory here is used to depoly pods across different vms, we suggest persistent volume, cpu and memory are ~20% smaller than a single vm, so each vm has one pod. The usage of the script can be listed using `./tg` command. Here, we used the default namesapce `default`.
```bash
git clone https://github.com/tigergraph/ecosys.git
cd ecosys/k8s
./tg gke kustomize -s 2 --pv 280 --cpu 30 --mem 200 -l [license string]
# for GKE
kubectl apply -f ./deploy/tigergraph-gke.yaml 
# for EKS
kubectl apply -f ./deploy/tigergraph-eks.yaml 
```

## Verify deployment
Depolyment can take several minutes. Use `kubectl get pod` to verify the deployment. An example output is
```
NAME              READY   STATUS    RESTARTS   AGE
installer-cztjf   1/1     Running   0          5m23s
tigergraph-0      1/1     Running   0          5m24s
tigergraph-1      1/1     Running   0          3m11s
``` 
## Download data
Fill in the parameters in `vars.sh`. Run the following script to start a background process in each pod to download data 
```bash
./download.sh
```
To check if the data is downloaded successfully 
```bash
kubectl exec -it tigergraph-0 -- bash
grun all 'tail ~/log.download' # last line should be 'download and decompress finished'
grun all 'du -sh  ~/tigergraph/data/sf100/' # The data should be in correct size
```

## Run benchmark 
First log into the k8s cluster 
```bash
kubectl exec -it tigergraph-0 -- bash
```

In the container, run (It is recommended to run scripts in the background because it usualy takes long time for large scale factors)
```bash
nohup ./k8s/setup.sh > log.setup 2>&1 < /dev/null &
```

To run queries
```bash
nohup ./k8s/queries.sh > log.queries 2>&1 < /dev/null &
```

To run batch updates
```bash
nohup ./k8s/batches.sh > log.batches 2>&1 < /dev/null &
```

To run benchmark (queries and batch updates)
```bash
nohup ./k8s/benchmark.sh > log.benchmark 2>&1 < /dev/null &
```

The outputs are in `~/output` on pod `tigergraph-0`. To download, first compress using tar `tar -cvf output.tar log.benchmark output/` then `kubectl cp tigergraph-0:output.tar output.tar`

To reset database
```bash
gsql drop all
```

## Release the cluter
```bash
# to delete the K8S pods and volumes
kubectl delete all -l app=tigergraph
kubectl delete pvc -l app=tigergraph
kubectl delete namespace -n default
# to delete GKE cluster
gcloud container clusters delete snb-bi-tg
```