#!/bin/bash

# Azure deploy:
export TF_VAR_DB_PASSWORD=${TF_VAR_DB_PASSWORD:-`pwgen 14 1`}
export COMMIT_ID=`git rev-parse HEAD`
terraform -chdir=./azure/ apply
terraform -chdir=./azure/ output -raw acr_username ; echo
terraform -chdir=./azure/ output -raw acr_password ; echo
terraform -chdir=./azure/ output -raw kube_config  ; echo
az aks get-credentials --resource-group azure-k8stest --name k8stest --admin --overwrite-existing
echo "Waiting 60 seconds for ACR dns name to get ready ..."
sleep 60
az acr login --name 135577b0-2610-495c-bee0-6150c858a2df
docker build -t nhlapi:$COMMIT_ID ./app/
docker tag nhlapi:$COMMIT_ID k8stest-5cdc4cbc.hcp.centralus.azmk8s.io/nhltop:$COMMIT_ID
docker push k8stest-5cdc4cbc.hcp.centralus.azmk8s.io/nhltop:$COMMIT_ID
kubectl create ns dev
kubectl create ns prod
kubectl create ns monitoring
sed -s -e "s/{{ DB_NAME }}/test/"          \
       -e "s/{{ NAMESPACE }}/dev/"         \
       -e "s/{{ COMMIT_ID }}/$COMMIT_ID/"  \
       ./k8s/deploy-tpl.yaml | kubectl -n dev apply -f -
sed -s -e "s/{{ DB_NAME }}/nhlapi/"        \
       -e "s/{{ NAMESPACE }}/prod/"        \
       -e "s/{{ COMMIT_ID }}/$COMMIT_ID/"  \
       ./k8s/deploy-tpl.yaml | kubectl -n prod apply -f -
kubectl -n dev  create secret generic appsec --from-literal=db_password=$TF_VAR_DB_PASSWORD
kubectl -n prod create secret generic appsec --from-literal=db_password=$TF_VAR_DB_PASSWORD

# Monitoring:
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install prometheus prometheus-community/kube-prometheus-stack -n monitoring
kubectl apply -f ./k8s/sm.yaml
kubectl get secret -n monitoring prometheus-grafana \
       	-o jsonpath={.data.admin-user} | base64 -d ; echo
kubectl get secret -n monitoring prometheus-grafana \
	-o jsonpath={.data.admin-password} | base64 -d ; echo
echo "Waiting 60 seconds for grafana and prometheus startup..."
sleep 60
kubectl -n monitoring port-forward \
	svc/prometheus-kube-prometheus-prometheus \
	--address 0.0.0.0 9090 &
kubectl -n monitoring port-forward \
	svc/prometheus-grafana \
	--address 0.0.0.0 3000:80 &

cat <<EOF
Development environment:

    http://epam-diploma.eastus.cloudapp.azure.com/

Production environment:

    http://epam-diploma-prod.eastus.cloudapp.azure.com/

EOF
