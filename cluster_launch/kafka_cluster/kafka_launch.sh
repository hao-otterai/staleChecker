CUR_DIR=$(dirname ${BASH_SOURCE})

CLUSTER_NAME=kellie-kafka-cluster

peg up ${CUR_DIR}/kafka_master.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} environment
peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} kafka
peg sshcmd-cluster ${CLUSTER_NAME} "pip install smart_open kafka pyspark boto3 botocore termcolor"

wait 
peg service ${CLUSTER_NAME} zookeeper start

wait
peg service ${CLUSTER_NAME} kafka start

