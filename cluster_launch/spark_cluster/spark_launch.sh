CUR_DIR=$(dirname ${BASH_SOURCE})

CLUSTER_NAME=kellie-spark-cluster

peg up ${CUR_DIR}/spark_master.yml &
peg up ${CUR_DIR}/spark_workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} environment
peg install ${CLUSTER_NAME} hadoop
peg install ${CLUSTER_NAME} spark

wait 
peg service ${CLUSTER_NAME} hadoop start
peg service ${CLUSTER_NAME} spark start

peg sshcmd-cluster ${CLUSTER_NAME} "pip install kafka pyspark boto3 botocore termcolor nltk mmh3 redis redis-py-cluster"
peg sshcmd-cluster ${CLUSTER_NAME} "python -m nltk.downloader wordnet"