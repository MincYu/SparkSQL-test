set -euxo pipefail
# This is for cluster with a master and two workers.
# test bandwidth and do the experiments between two workers

# https://docs.aws.amazon.com/zh_cn/AWSEC2/latest/UserGuide/enhanced-networking-ena.html
# Before beginning this experiment, refer to the above site to enable EMA.

# When the machines are in stop states:
# aws ec2 modify-instance-attribute --instance-id <instance-id> --ena-support

SCALE=1
QUERY=30

test_bandwidth(){
    cluster_name=$1
    logs_dir=$2

    echo "----------TESTING BANDWIDTH----------"
    echo "Install iperf3 if needed"
    flintrock run-command $cluster_name "sudo yum -y install iperf3"

    echo "Setup iperf3 sever on a worker"
    flintrock run-command --master-only $cluster_name 'workers=(`cat /home/ec2-user/hadoop/conf/slaves`); ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "iperf3 -s > /dev/null 2>&1 &"'

    echo "Setup iperf3 client on a worker and test bandwith between workers"
    flintrock run-command --master-only $cluster_name 'workers=(`cat /home/ec2-user/hadoop/conf/slaves`); ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "iperf3 -c ${workers[0]} -d" >> /home/ec2-user/logs/'${logs_dir}'/bandwith.txt'

    # echo "Download the results file"

    # flintrock run-command --master-only $cluster_name 'cat /home/ec2-user/hadoop/conf/masters'
    echo "kill iperf3 server process"
    flintrock run-command --master-only $cluster_name 'workers=(`cat /home/ec2-user/hadoop/conf/slaves`); ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "pkill iperf3"'
}

test_all(){
    cluster_name=$1
    datetime=$(date +%Y%m%d-%H%M%S)
    logs_dir=scale${SCALE}_query${QUERY}_${datetime}

    flintrock run-command --master-only $cluster_name 'mkdir -p /home/ec2-user/logs/'${logs_dir}''

    flintrock run-command $cluster_name 'sed -i "/alluxio.user.file.passive.cache.enabled=false/c\alluxio.user.file.passive.cache.enabled=true" /home/ec2-user/alluxio/conf/alluxio-site.properties'

    echo "Restart alluxio & hdfs"
    flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/restart.sh'

    test_bandwidth $cluster_name $logs_dir

    flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/auto-test-2.sh pre '${SCALE}' '${QUERY}''
    flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/auto-test-2.sh all '${SCALE}' '${QUERY}' > /home/ec2-user/logs/'${logs_dir}'/autotest.log'

    flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/auto-test-2.sh clean '${SCALE}' '${QUERY}''

    flintrock run-command --master-only $cluster_name 'mv /home/ec2-user/logs/noshuffle /home/ec2-user/logs/'${logs_dir}'/'
    flintrock run-command --master-only $cluster_name 'mv /home/ec2-user/logs/shuffle /home/ec2-user/logs/'${logs_dir}'/'

}

test_all_wapper(){
    cluster_name=$1

    for((i=10;i<=20;i=i+5)); do
        SCALE=$i
        test_all $cluster_name
    done
}

if [[ "$#" -lt 2 ]]; then
    echo "Usage: $0 all/shuffle/nonshuffle <cluster name>"
    exit 1
else
    case $1 in
        all)                    test_all_wapper $2
                                ;;
        shuffle)                test_shuffle $2
                                ;;
        nonshuffle)             test_noshuffle $2
                                ;;         
        * )                     usage
    esac
fi

