ACR_NAME=benediktubuntudev
az acr build --registry $ACR_NAME --image kafka_e2e_test:v0.8 --file docker/Dockerfile docker

