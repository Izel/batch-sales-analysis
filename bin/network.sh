#!/bin/zsh

# Nombre de la nueva Red VPC
VPC_NAME="dataproc-serverless-vpc" 
# Región específica (europe-east2 en Rumania)
REGION="europe-west2" 
# Nombre de la Subred dentro de esa región
SUBNET_NAME="dataproc-serverless-subnet" 
# Rango de IP que usará esta subred (Ejemplo: un /20)
SUBNET_IP_RANGE="10.10.0.0/20"

echo "Creando la Red VPC: ${VPC_NAME}..."
gcloud compute networks create ${VPC_NAME} \
    --subnet-mode=custom \
    --description="VPC para Dataproc Serverless en europe-east2"

echo "Creando la Subred: ${SUBNET_NAME} en ${REGION} con rango ${SUBNET_IP_RANGE}..."
gcloud compute networks subnets create ${SUBNET_NAME} \
    --network=${VPC_NAME} \
    --range=${SUBNET_IP_RANGE} \
    --region=${REGION} \
    --enable-flow-logs \
    --stack-type=IPV4_ONLY \
    --description="Subred dedicada para Dataproc Serverless"

echo "Habilitando el Acceso Privado de Google (PGA)..."
gcloud compute networks subnets update ${SUBNET_NAME} \
    --region=${REGION} \
    --enable-private-ip-google-access

echo "Creando regla de Firewall para tráfico interno..."
gcloud compute firewall-rules create ${VPC_NAME}-allow-internal \
    --network=${VPC_NAME} \
    --action=ALLOW \
    --direction=INGRESS \
    --priority=1000 \
    --rules=all \
    --source-ranges=${SUBNET_IP_RANGE}

echo "Creando regla de Firewall para tráfico de salida a Internet..."
gcloud compute firewall-rules create ${VPC_NAME}-allow-egress \
    --network=${VPC_NAME} \
    --action=ALLOW \
    --direction=EGRESS \
    --priority=1000 \
    --rules=all \
    --destination-ranges=0.0.0.0/0