CUR_DIR=$(dirname ${BASH_SOURCE})

CLUSTER_NAME=kellie-redis-cluster

peg up ${CUR_DIR}/redis_master.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} environment
peg install ${CLUSTER_NAME} redis

wait 
peg sshcmd-cluster ${CLUSTER_NAME} "sudo apt install redis-server"

wait 
peg sshcmd-cluster ${CLUSTER_NAME} "sudo apt install redis-tools"

wait
peg sshcmd-clusters ${CLUSTER_NAME} "pip install redis redis-py-cluster"

wait
peg service ${CLUSTER_NAME} redis start
