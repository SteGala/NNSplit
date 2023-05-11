# Build image

docker build -t stegala/acbba ./docker

# Create kind environment

kind create cluster --name k8s-playground --config ./misc/kind-config.yaml
