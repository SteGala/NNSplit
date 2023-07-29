# Build image

docker build -t stegala/acbba ./docker

# Create kind environment

kind create cluster --name k8s-playground --config ./misc/kind-config.yaml

# Notes:

In the definition of the CR, the NN cannot request momre than 1000GPU, 1000CPU, and 1G of bandwidth
