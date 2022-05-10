# benchmark on K8S cluster
## Overview
Benchmarks on clusters are performed using [kubernetes (k8s)](https://kubernetes.io). Cluster is created using GKE (Google Kubernetes Engine) on Google Cloud, or EKS on AWS. 
Pre-requisites are
* `kubectl`
* command line tool for GCP or AWS: `gcloud` or `aws-cli`. The default project and region/zone need to be configured. For GCP, is `gcloud init`.

> Benchmark can also be performed on local clusters without k8s. But the setup is susceptible to errors and safety issues. Brief instructions are in [Section: Benchmark without k8s](##Benchmark_without_k8s)
## Create the cluster
Create [GKE container cluster](https://cloud.google.com/sdk/gcloud/reference/container/clusters/create) specifying machine type, number of nodes, disk size and disk type. For example,  
```bash
gcloud container clusters create snb-bi-tg --machine-type n2-highmem-32 --num-nodes=2 --disk-size 300 --disk-type=pd-ssd
```
Or create [EKS cluster](https://docs.aws.amazon.com/eks/latest/userguide/create-cluster.html),
```bash
eksctl create cluster --name test --region us-east-2 --nodegroup-name tgtest --node-type r5.xlarge --nodes 2 --instance-prefix tg --instance-name eks-test 
```

## Deploy TG containers
Deply the containers using the script `k8s/tg` from [tigergraph/ecosys](https://github.com/tigergraph/ecosys.git). The recommended value for persistent volume, cpu and memory are ~20% smaller than those of a single machine. Thus, each machine has exactly one pod.
```bash
git clone https://github.com/tigergraph/ecosys.git
cd ecosys/k8s
./tg gke kustomize -s 2 --pv 280 --cpu 30 --mem 200 -l [license string]
kubectl apply -f ./deploy/tigergraph-gke.yaml
```

Or on EKS 
```bash
./tg eks kustomize -s 2 --pv 280 --cpu 30 --mem 200 -l [license string]
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
1. Fill in the parameters in `vars.sh`.
    * `NUM_NODES` - number of nodes.
    * `SF` - data source, choices are 100, 300, 1000, 3000, 10000.
    * `DOWNLOAD_THREAD` - number of download threads
    * `TG_HEADER` - whether the data has header, should be true
1. Run:
    ```bash
    ./download.sh
    ```
    It will start background processes on each pods to download and decompress the data.

1. To check if the data is downloaded successfully, log into the cluster using `kubectl exec -it tigergraph-0 -- bash` and then run
    ```bash
    grun all 'tail ~/log.download' # last line should be 'download and decompress finished'
    grun all 'du -sh  ~/tigergraph/data/sf*/' # The data should be SF / NODE_NUMBER
    ```

## Run Benchmark 
1. log into the k8s cluster 
    ```bash
    kubectl exec -it tigergraph-0 -- bash
    ```

1. In the container, run (It is recommended to run scripts in the background because it usualy takes long time for large scale factors)
   ```bash
   nohup ./k8s/setup.sh > log.setup 2>&1 < /dev/null &
   ```

1. To run benchmark scripts
    ```bash
    nohup ./k8s/benchmark.sh > log.benchmark 2>&1 < /dev/null &
    ```

    The `queries.sh` and `batches.sh` can be run in the similar approach. The outputs are in `~/output`. To download, 
    * Compress using tar `tar -cvf output.tar log.benchmark output/` 
    * On local desktop, `kubectl cp tigergraph-0:output.tar output.tar`

1. To reset TigerGraph database
    ```bash
    gsql drop all
    nohup ./k8s/setup.sh > log.setup 2>&1 < /dev/null &
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