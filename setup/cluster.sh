set -euxo pipefail
# This script is for one master HDFS, alluxio, spark cluster deployment.

# Prerequisites:
#     1. Install flintrock:
#         https://github.com/nchammas/flintrock#installation
#     2. After 1, make sure you have an AWS account, set your AWS Key Info in your shell and run "flintrock configure" to configure your cluster(Pls refer to https://heather.miller.am/blog/launching-a-spark-cluster-part-1.html#setting-up-flintrock-and-amazon-web-services)
manual_restart(){
	cluster_name=$1

	echo "stop all"
	flintrock run-command --master-only $cluster_name '/home/ec2-user/spark/sbin/stop-all.sh;/home/ec2-user/alluxio/bin/alluxio-stop.sh all;/home/ec2-user/hadoop/sbin/stop-dfs.sh;'

	echo "configure"
	flintrock run-command $cluster_name 'echo "export JAVA_HOME="/home/ec2-user/jdk1.8.0_201"" >> /home/ec2-user/hadoop/conf/hadoop-env.sh;'

	flintrock run-command --master-only $cluster_name '/home/ec2-user/hadoop/bin/hdfs namenode -format -nonInteractive || true;'
	
	echo "restart all"
	flintrock run-command --master-only $cluster_name '/home/ec2-user/hadoop/sbin/start-dfs.sh; /home/ec2-user/alluxio/bin/alluxio format; /home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount; /home/ec2-user/spark/sbin/start-all.sh;'

	echo "manual restart finnished"
}
configure_alluxio(){
	# configure alluxio
	cluster_name=$1
	echo "Configure alluxio"

	flintrock run-command $cluster_name 'cd /home/ec2-user; cp /home/ec2-user/alluxio/conf/alluxio-site.properties.template /home/ec2-user/alluxio/conf/alluxio-site.properties'

	flintrock run-command $cluster_name 'echo "alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	# flintrock run-command $cluster_name 'echo "alluxio.user.file.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	# flintrock run-command $cluster_name 'echo "alluxio.worker.hostname=localhost" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	flintrock run-command $cluster_name 'echo "alluxio.user.file.delete.unchecked=true" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	flintrock run-command $cluster_name 'echo "alluxio.user.file.passive.cache.enabled=false" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	flintrock run-command $cluster_name 'echo "alluxio.user.file.replication.min=2" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	flintrock run-command $cluster_name 'echo "alluxio.worker.memory.size=26GB" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'

	flintrock run-command $cluster_name 'echo "alluxio.master.hostname=$(cat /home/ec2-user/hadoop/conf/masters)" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "alluxio.underfs.address=hdfs://$(cat /home/ec2-user/hadoop/conf/masters):9000/alluxio/root/" >> /home/ec2-user/alluxio/conf/alluxio-site.properties'


	flintrock run-command $cluster_name 'hadoop fs -mkdir -p /alluxio/root/'

	flintrock run-command $cluster_name 'cp /home/ec2-user/hadoop/conf/masters /home/ec2-user/alluxio/conf/masters;
	cp /home/ec2-user/hadoop/conf/slaves /home/ec2-user/alluxio/conf/workers'

}
launch() {
	cluster_name=$1

	echo "Launch cluster ${cluster_name}"
	# launch your specified cluster
	flintrock launch $cluster_name

	# delete java1.8 installed by flintrock
	flintrock run-command $cluster_name "sudo yum -y remove java-1.8.0-openjdk.x86_64 java-1.8.0-openjdk-headless.x86_64"

	# delete JAVA_HOME set by flintrock
	flintrock run-command $cluster_name 'echo `sed -e '/JAVA_HOME/d' /etc/environment` | sudo tee /etc/environment; source /etc/environment'


	# download Oracle JDK 1.8
	flintrock run-command $cluster_name 'wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" https://download.oracle.com/otn-pub/java/jdk/8u201-b09/42970487e3af4f5aa5bca3f542482c60/jdk-8u201-linux-x64.tar.gz;tar zxvf jdk-8u201-linux-x64.tar.gz;rm jdk-8u201-linux-x64.tar.gz'

	# set Env Variable for Oracle JDK 1.8
	flintrock run-command $cluster_name 'echo "export JAVA_HOME=\$HOME/jdk1.8.0_201" >> /home/ec2-user/.bashrc;
	echo "export JRE_HOME=\$JAVA_HOME/jre" >> /home/ec2-user/.bashrc;
	echo "export CLASSPATH=.:\$JAVA_HOME/lib:\$JRE_HOME/lib" >> /home/ec2-user/.bashrc;
	echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /home/ec2-user/.bashrc'

	# Register Oracle JDK 1.8
	echo "Register Oracle JDK 1.8"

	flintrock run-command $cluster_name 'sudo update-alternatives --install /usr/bin/java java /home/ec2-user/jdk1.8.0_201/bin/java 300;sudo update-alternatives --install /usr/bin/javac javac /home/ec2-user/jdk1.8.0_201/bin/javac 300;sudo update-alternatives --install /usr/bin/jar jar /home/ec2-user/jdk1.8.0_201/bin/jar 300;sudo update-alternatives --install /usr/bin/javah javah /home/ec2-user/jdk1.8.0_201/bin/javah 300;sudo update-alternatives --install /usr/bin/javap javap /home/ec2-user/jdk1.8.0_201/bin/javap 300;'

	# Install maven
	echo "Install maven"

	flintrock run-command $cluster_name 'wget http://mirrors.ocf.berkeley.edu/apache/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz;
	tar zxvf apache-maven-3.5.4-bin.tar.gz;
	rm apache-maven-3.5.4-bin.tar.gz
	'

	flintrock run-command $cluster_name 'echo "export MAVEN_HOME=\$HOME/apache-maven-3.5.4" >> /home/ec2-user/.bashrc;
	echo "export PATH=\$MAVEN_HOME/bin:\$PATH" >> /home/ec2-user/.bashrc'

	# Install git & setup
	echo "Install git"

	flintrock run-command $cluster_name 'sudo yum -y install git'

	flintrock run-command $cluster_name "sudo yum -y install iperf3"

	flintrock run-command $cluster_name 'mkdir -p /home/ec2-user/logs'


	echo "Install tools for master"

	flintrock run-command --master-only $cluster_name 'curl https://bintray.com/sbt/rpm/rpm > bintray-sbt-rpm.repo; 
	sudo mv bintray-sbt-rpm.repo /etc/yum.repos.d/;
	sudo yum -y install python-pip gcc make flex bison byacc sbt
	sudo pip install click'

	flintrock run-command --master-only $cluster_name 'echo "export PYTHONPATH=\$SPARK_HOME/python:\$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:\$PYTHONPATH" >> /home/ec2-user/.bashrc'

	# Download alluxio source code
	echo "Download & compile alluxio"

	flintrock run-command $cluster_name 'git clone git://github.com/CheneyYu96/alluxio.git'

	# Compile alluxio source code
	flintrock run-command $cluster_name 'cd /home/ec2-user/alluxio; git pull; mvn install -Phadoop-2 -Dhadoop.version=2.8.5 -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true'

	# Download workload & compile
	echo "Download & compile workload"
	flintrock run-command $cluster_name 'git clone git://github.com/CheneyYu96/tpch-spark.git'
	# flintrock run-command --master-only $cluster_name 'mv /home/ec2-user/SparkSQL-test /home/ec2-user/tpch-spark'
	flintrock run-command --master-only $cluster_name 'cd /home/ec2-user/tpch-spark/dbgen; make'
	flintrock run-command --master-only $cluster_name 'cd /home/ec2-user/tpch-spark/; sbt assembly'

	configure_alluxio $cluster_name

	# configure spark
	echo "Configure spark"

	flintrock run-command $cluster_name 'cp /home/ec2-user/spark/conf/spark-defaults.conf.template /home/ec2-user/spark/conf/spark-defaults.conf'
	flintrock run-command $cluster_name 'cp /home/ec2-user/spark/conf/log4j.properties.template /home/ec2-user/spark/conf/log4j.properties'

	flintrock run-command $cluster_name 'echo "spark.driver.extraClassPath /home/ec2-user/alluxio/client/$(ls /home/ec2-user/alluxio/client)" >> /home/ec2-user/spark/conf/spark-defaults.conf;
	echo "spark.executor.extraClassPath /home/ec2-user/alluxio/client/$(ls /home/ec2-user/alluxio/client)" >> /home/ec2-user/spark/conf/spark-defaults.conf'

	# set hadoop
	echo "Configure hadoop"

	flintrock run-command $cluster_name 'sed -i "/<\/configuration>/d" /home/ec2-user/hadoop/conf/core-site.xml;
	echo "
	  <property>
	    <name>fs.alluxio.impl</name>
	    <value>alluxio.hadoop.FileSystem</value>
	  </property>
	</configuration>" >> /home/ec2-user/hadoop/conf/core-site.xml
	'

	flintrock run-command $cluster_name 'echo "export HADOOP_CLASSPATH=/home/ec2-user/alluxio/client/$(ls /home/ec2-user/alluxio/client):\$HADOOP_CLASSPATH" >> /home/ec2-user/.bashrc; source /home/ec2-user/.bashrc'

	# flintrock run-command --master-only $cluster_name '/home/ec2-user/hadoop/sbin/stop-dfs.sh;/home/ec2-user/hadoop/sbin/start-dfs.sh;/home/ec2-user/alluxio/bin/alluxio format;/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount'
	
	# echo "setup wondershaper for bandwidth limitation"
	flintrock run-command $cluster_name "sudo yum -y install tc; git clone  https://github.com/magnific0/wondershaper.git; cd wondershaper; sudo make install;" # sudo systemctl enable wondershaper.service; sudo systemctl start wondershaper.service"

	# restart 
	echo "Restart"

	# flintrock stop --assume-yes $cluster_name
	# start $cluster_name
	manual_restart $cluster_name

}

start() {
	cluster_name=$1

	echo "Start cluster ${cluster_name}"
	flintrock start $cluster_name

	echo "Configure alluxio"
	flintrock run-command $cluster_name 'cp /home/ec2-user/hadoop/conf/masters /home/ec2-user/alluxio/conf/masters;
	cp /home/ec2-user/hadoop/conf/slaves /home/ec2-user/alluxio/conf/workers'

	flintrock run-command $cluster_name 'sed -i "\$d" /home/ec2-user/alluxio/conf/alluxio-site.properties;sed -i "\$d" /home/ec2-user/alluxio/conf/alluxio-site.properties;'

	flintrock run-command $cluster_name 'echo "alluxio.master.hostname=$(cat /home/ec2-user/hadoop/conf/masters)" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "alluxio.underfs.address=hdfs://$(cat /home/ec2-user/hadoop/conf/masters):9000/alluxio/root/" >> /home/ec2-user/alluxio/conf/alluxio-site.properties'

	echo "Restart alluxio & hdfs"
	flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/alluxio-stop.sh all;/home/ec2-user/hadoop/sbin/stop-dfs.sh;/home/ec2-user/hadoop/sbin/start-dfs.sh;/home/ec2-user/alluxio/bin/alluxio format;/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount'
}

stop() {
	cluster_name=$1
	echo "Stop cluster ${cluster_name}"

	flintrock stop --assume-yes $cluster_name
}

destroy() {
	cluster_name=$1
	echo "Destory cluster ${cluster_name}"

	flintrock destroy $cluster_name
}

updata_alluxio() {
	cluster_name=$1
	echo "Update & Recompile alluxio for cluster ${cluster_name}"

	echo "Pull alluxio for repo"
	flintrock run-command $cluster_name 'cd /home/ec2-user/alluxio; git checkout conf/threshold; git pull'

	echo "Compile ... "
	flintrock run-command $cluster_name 'cd /home/ec2-user/alluxio; mvn install -Phadoop-2 -Dhadoop.version=2.8.5 -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true'

	configure_alluxio $cluster_name
	
	echo "Restart alluxio & hdfs"
	flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/alluxio-stop.sh all;/home/ec2-user/hadoop/sbin/stop-dfs.sh;/home/ec2-user/hadoop/sbin/start-dfs.sh;/home/ec2-user/alluxio/bin/alluxio format;/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount'
}

usage() {
    echo "Usage: $0 start|stop|launch|destroy|update|conf_alluxio <cluster name>"
}

if [[ "$#" -lt 2 ]]; then
    usage
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
        update)					updata_alluxio $2
        						;;
		conf_alluxio)			configure_alluxio $2
								;;             
		man_start)				manual_restart $2
								;;
        * )                     usage
    esac
fi

