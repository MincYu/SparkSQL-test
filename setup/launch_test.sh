


if [[ "$#" -lt 1 ]]; then
    echo "Usage: $0 <cluster name>"
    exit 1
fi

cluster_name=$1

flintrock run-command --master-only $cluster_name 'nohup ./home/ec2-user/tpch-spark/setup/experiment2.sh all > /home/ec2-user/nohup.out 2>&1 &'
