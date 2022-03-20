# TigerGraph implementation on cluster using K8S
## Overview
The benchmark for SF-1k and larger are performed in cluster mode and impletemented using (kubernetes)[https://kubernetes.io] (K8S). The cluster can be easily created on Google Kubernetes Engine (GKE) on Google Cloud Platform (GCP),  or Amazon Elastic Kubernetes Service (EKS) on Amazon Web Service (AWS). 

Pre-requisites on local destop
* `kubectl`
* command line tool for GCP or AWS: `gcloud` or `aws-cli`. 

## Create the cluster
On GKE, 
```bash
gcloud container clusters create [cluster name] -m [machine type] --num-nodes=[number of nodes] --disk-size [disk size in GB] --disk-type=[disk type]
```
For example, the following create a 2-node cluster,
```bash
gcloud container clusters create snb-tg -m n2-highmem-16 --num-nodes=2 --disk-size 100 --disk-type=pd-ssd
```
On EKS 
```bash
eksctl create cluster --name test --region us-east-2 --nodegroup-name tgtest --node-type r5.xlarge --nodes 2 --instance-prefix tg --instance-name eks-test 
```

## deploy the containers
First, deply the containers on the cluter using a script `tg` from `tigergraph/ecosys` repo. The usage of the script can be listed using `tg` command. Here, we used the default namesapce, which is `default`.
```bash
git clone https://github.com/tigergraph/ecosys.git
cd ecosys/k8s
./tg gke kustomize -s 2 --pv 200 --cpu 8 --mem 80 -l [license string]
# for GKE
kubectl apply -f ./deploy/tigergraph-gke.yaml 
# for EKS
kubectl apply -f ./deploy/tigergraph-eks.yaml 
```

## Verify deployment
Depolyment can take several minutes. Use `kubectl get pod` to verify the deployment. An example output is
```
NAME               READY   STATUS    RESTARTS   AGE
pod/tigergraph-0   1/1     Running   0          11m
pod/tigergraph-1   1/1     Running   0          9m20s
``` 
## Download data
The following scripts start a background process in each pod to download data 
```bash
export n=2 #number of pods or nodes
export sf=1k 
export thread=1
cd ..
tar -cf tmp.tar queries ddl dml k8s bi.py batches.py
cd k8s

for i in $( seq 0 $((n-1)) ); do
  echo "tigergraph-$i: Upload scripts"
  kubectl cp ../tmp.tar tigergraph-${i}:tmp.tar 
  echo "tigergraph-$i: Start download"  
  kubectl exec tigergraph-${i} -- bash -c "tar -xf tmp.tar; bash download.sh  $sf $i $n $thread > log.download 2> /dev/null &"  
done
```

To check the data downloading status use
```bash
for i in $( seq 0 $((n-1)) ); do
  echo "tigergraph-$i:"
  kubectl exec tigergraph-${i} -- bash -c "tail -1 log.download"
done
```

## Run benchmark 

First log into the 
```bash
kubectl exec -i --tty tigergraph-0 -- bash
```



## Clean up
```bash
# to delete the K8S pods and volumes
kubectl delete all -l app=tigergraph
kubectl delete pvc -l app=tigergraph
kubectl delete namespace -n default
# to delete GKE cluster
gcloud container clusters delete snb-tg
```