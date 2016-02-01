# Setting up a Kubernetes cluster

There are various guides on how to run Kubernetes clusters on a wide variety of
hardware (various cloud solutions, bare metal, etc.). The following instructions
should work similarly on all of these, we encourage you to consult
[https://github.com/kubernetes/kubernetes/blob/release-1.1/docs/getting-started-guides/].

For EC2, you should download and run Kubernetes using
```
export KUBERNETES_PROVIDER=aws; wget -q -O - https://get.k8s.io | bash
```
or if it is already downloaded with the `./kube-up.sh` script:
```
export MASTER_SIZE=m3.medium
export MINION_SIZE=m3.medium
./kube-up.sh
```

# Launching Orchestra

We provide a Docker image with Orchestra under
[https://hub.docker.com/r/pcmoritz/orchestra/]. It can be started with

```
./kubectl.sh run master --image=pcmoritz/orchestra:pre
```

# Creating and uploading docker images

To create the docker image, run `docker build .` in the directory that contains
the `Dockerfile`.

You can then start a container that runs the image by executing
```
docker run -t -i <image-id>
```

Run `docker ps` to get the id of the container and commit the image using
```
docker commit -m "<message>" -a "<commiter>" <container-id> pcmoritz/orchestra:pre
```

The image can then be uploaded by running
```
docker login
docker push pcmoritz/orchestra:pre
```
