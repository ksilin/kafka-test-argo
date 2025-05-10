kubectl create secret docker-registry acr-pull-secret \
  --docker-server=$ACR_URL \
  --docker-username=$ACR_USER \
  --docker-password=$ACR_PASS \
  --namespace=$NAMESPACE
