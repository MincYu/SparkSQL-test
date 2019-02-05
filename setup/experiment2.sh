set -euxo pipefail
# This is for cluster with a master and two workers.
# test bandwidth and do the experiments between two workers

# https://docs.aws.amazon.com/zh_cn/AWSEC2/latest/UserGuide/enhanced-networking-ena.html
# Before beginning this experiment, refer to the above site to enable EMA.

# When the machines are in stop states:
# aws ec2 modify-instance-attribute --instance-id <instance-id> --ena-support
SCALE=1
QUERY=30
BAND_LIMIT=550000

workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
echo "worker1: ${workers[0]}"
echo "worker2: ${workers[1]}"

test_bandwidth(){

    logs_dir=$1

    echo "----------TESTING BANDWIDTH----------"
    echo "Install iperf3 if needed"
    sudo yum -y install iperf3

    echo "Setup iperf3 sever on a worker"
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "iperf3 -s > /dev/null 2>&1 &"

    echo "Setup iperf3 client on a worker and test bandwith between workers"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "iperf3 -c ${workers[0]} -d" >> /home/ec2-user/logs/${logs_dir}/bandwith.txt

    echo "kill iperf3 server process"
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "pkill iperf3"
}

test_all(){

    datetime=$(date +%Y%m%d-%H%M%S)
    logs_dir=scale${SCALE}_query${QUERY}_${datetime}

    echo "Create directory for this experiment:"
    mkdir -p /home/ec2-user/logs/${logs_dir}

    echo "Begin to test noshuffle scenario"
    sed -i "/alluxio.user.file.passive.cache.enabled=false/c\alluxio.user.file.passive.cache.enabled=true" /home/ec2-user/alluxio/conf/alluxio-site.properties

    # /home/ec2-user/alluxio/bin/alluxio copyDir /home/ec2-user/alluxio/conf
    # This statement will be executed in restart.sh

    echo "Restart alluxio & hdfs"
    /home/ec2-user/alluxio/bin/restart.sh

    test_bandwidth $logs_dir

    echo "Begin Testing"
    /home/ec2-user/alluxio/bin/auto-test.sh pre ${SCALE} ${QUERY}

    /home/ec2-user/alluxio/bin/auto-test.sh all ${SCALE} ${QUERY} > /home/ec2-user/logs/${logs_dir}/autotest.log

    /home/ec2-user/alluxio/bin/auto-test.sh clean ${SCALE} ${QUERY}

    echo "Copy Logs"
    mv /home/ec2-user/logs/noshuffle /home/ec2-user/logs/${logs_dir}/
    mv /home/ec2-user/logs/shuffle /home/ec2-user/logs/${logs_dir}/


}

test_all_wapper(){

    # Set bandwidth limit
    echo "Set bandwidth limit"
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0; sudo wondershaper -a eth0 -d $BAND_LIMIT -u $BAND_LIMIT"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0; sudo wondershaper -a eth0 -d $BAND_LIMIT -u $BAND_LIMIT"

    for((i=1;i<=10;i=i+1)); do
        SCALE=$i
        test_all
    done

    echo "Eliminate bandwidth limit"
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0"
    echo "All tasks done!!"
}

if [[ "$#" -lt 1 ]]; then
    echo "Usage: $0 all/shuffle/nonshuffle <cluster name>"
    exit 1
else
    case $1 in
        all)                    test_all_wapper
                                ;;         
        * )                     usage
    esac
fi