# This script is run on laptops.

launch(){
    cluster_name=$1
    flintrock launch $cluster_name

    flintrock run-command --master-only $cluster_name 'wget https://raw.githubusercontent.com/CheneyYu96/SparkSQL-test/master/setup/cluster2.sh; chmod +x cluster2.sh'

    flintrock run-command --master-only $cluster_name 'nohup /home/ec2-user/cluster2.sh launch > /home/ec2-user/nohup.out 2>&1 &'
}
start(){
    cluster_name=$1
    echo "Start cluster ${cluster_name}"
    flintrock start $cluster_name

    flintrock run-command --master-only $cluster_name 'nohup /home/ec2-user/cluster2.sh start > /home/ec2-user/nohup.out 2>&1'
}

stop(){
    cluster_name=$1
    flintrock stop $cluster_name
}

destroy() {
	cluster_name=$1
	echo "Destory cluster ${cluster_name}"

	flintrock destroy $cluster_name
}

updata_alluxio(){
    cluster_name=$1
	echo "Update & Recompile alluxio for cluster ${cluster_name}"

    flintrock run-command --master-only $cluster_name 'nohup /home/ec2-user/cluster2.sh update > /home/ec2-user/nohup.out 2>&1'
}

configure_alluxio(){
    cluster_name=$1
	echo "Configure alluxio"

    flintrock run-command --master-only $cluster_name 'nohup /home/ec2-user/cluster2.sh conf_alluxio > /home/ec2-user/nohup.out 2>&1'
}

if [[ "$#" -lt 2 ]]; then
    echo "Usage: $0 <cluster name>"
    exit 1
else
    case $1 in
        start)                  start $2
                                ;;
        stop)                   stop $2
                                ;;
        launch)                	launch $2
                                ;;
        destroy)				destroy $2
        						;;
        update)					    updata_alluxio $2
        						;;
		conf_alluxio)			configure_alluxio $2
								;;             
        * )                     usage
    esac
fi
