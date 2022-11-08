# benchmark on K8S cluster

## Overview

Benchmarks on clusters are performed using [kubernetes (k8s)](https://kubernetes.io). Cluster is created using GKE (Google Kubernetes Engine) on Google Cloud, or EKS on AWS. 
Pre-requisites are
* `kubectl`
* command line tool for GCP or AWS: `gcloud` or `aws-cli`. The default project and region/zone need to be configured. For GCP, is `gcloud init`.

> Benchmark can also be performed on local clusters without k8s. But the setup is susceptible to errors and safety issues. Brief instructions are in [Section: Benchmark without k8s](../benchmark_on_cluster).

## Create the cluster

Create [GKE container cluster](https://cloud.google.com/sdk/gcloud/reference/container/clusters/create) specifying machine type, number of nodes, disk size and disk type. 
Use SF1000 benchmark as an example:

```bash
gcloud container clusters create snb-bi-tg --machine-type n2-highmem-32 --num-nodes=4 --disk-size 700 --disk-type=pd-ssd
```

Or create [EKS cluster](https://docs.aws.amazon.com/eks/latest/userguide/create-cluster.html), (the cluster creation takes long time)

```bash
eksctl create cluster --name sf1000 --region us-east-2 --nodegroup-name tgtest --node-type r6a.8xlarge --nodes 4 --instance-prefix tg --instance-name eks-test
```

## Deploy TG containers
Deploy the containers using the script `k8s/tg` from [tigergraph/ecosys](https://github.com/tigergraph/ecosys.git). The recommended value for persistent volume, cpu and memory are ~20% smaller than those of a single machine. Thus, each machine has exactly one pod.

On GKE
```
git clone https://github.com/tigergraph/ecosys.git
cd ecosys/k8s
./tg gke kustomize -v 3.7.0 -n tigergraph -s 4 --pv 700 --cpu 30 --mem 200 -l [license string]
kubectl apply -f ./deploy/tigergraph-eks-tigergraph.yaml
```

Or on EKS 

Important: If you have a 1.22 or earlier cluster that you currently run pods on that use Amazon EBS volumes, and you don't currently have this driver installed on your cluster, then be sure to install this driver to your cluster before updating the cluster to 1.23.

Following the instructions on [AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/managing-ebs-csi.html) to add EBS CSI add-on before proceed.


Use ```kubectl get pods -n kube-system``` to check if EBS CSI driver is running. An example output is
```
NAME                                  READY   STATUS    RESTARTS        AGE
...
coredns-5948f55769-kcnvx              1/1     Running   0               3d6h
coredns-5948f55769-z7mbr              1/1     Running   0               3d6h
ebs-csi-controller-75598cd6f4-48dp8   6/6     Running   0               3d4h
ebs-csi-controller-75598cd6f4-sqbhw   6/6     Running   4 (2d11h ago)   3d4h
ebs-csi-node-9cmbj                    3/3     Running   0               3d4h
ebs-csi-node-g65ns                    3/3     Running   0               3d4h
ebs-csi-node-qzflk                    3/3     Running   0               3d4h
ebs-csi-node-x2t22                    3/3     Running   0               3d4h
...
```

If the ebs csi driver is installed, then run (here use "tigergraph" as namespace for an example)
```bash
kubectl create ns tigergraph
git clone https://github.com/tigergraph/ecosys.git
cd ecosys/k8s
./tg eks kustomize -v 3.7.0 -n tigergraph -s 4 --pv 700 --cpu 30 --mem 200 -l [license string]
kubectl apply -f ./deploy/tigergraph-eks-tigergraph.yaml
```


## Verify deployment

Deployment can take several minutes. Use `kubectl get pod -n tigergraph` to verify the deployment. An example output is

```
NAME              READY   STATUS    RESTARTS   AGE
installer-cztjf   1/1     Running   0          5m23s
tigergraph-0      1/1     Running   0          5m24s
tigergraph-1      1/1     Running   0          3m11s
...
``` 
Alternative verification commands include:
```
kubectl get all -n tigergraph
kubectl describe pod/tigergraph-0 -n tigergraph
kubectl describe pvc -n tigergraph

```


## Download data
To download the data, service key json file must be located in ```k8s/``` . The bucket is public now and any service key should work.
1. Fill in the parameters in `vars.sh`.
    * `NUM_NODES` - number of nodes.
    * `SF` - data source, choices are 100, 300, 1000, 3000, 10000.
    * `DOWNLOAD_THREAD` - number of download threads    
         


         
1. Put your own service key file in ```tigergraph/``` folder.
    
    Our bucket is public, and any google cloud service key is able to access the data. To create service key, refer to Google Cloud documentation.
    

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

1. Log into the k8s cluster 

    ```bash
    kubectl exec -it tigergraph-0 -- bash
    ```

1. In the container, run the following command. (It is recommended to run scripts in the background because it usualy takes long time for large scale factors.)

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

1. To reset TigerGraph database:

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
# to delete EKS cluster
kubectl delete svc [service-name]
eksctl delete cluster --name [yourclustername]
# to delete GKE cluster
gcloud container clusters delete snb-bi-tg
```
