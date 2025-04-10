ACR_NAME=kafkae2etest
az acr build --registry $ACR_NAME --image kafka_e2e_test:v0.1 --file docker/Dockerfile docker

