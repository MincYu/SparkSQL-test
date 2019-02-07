set -euxo pipefail
# This script is for one master HDFS, alluxio, spark cluster deployment.

# Prerequisites:
#     1. Install flintrock:
#         https://github.com/nchammas/flintrock#installation
#     2. After 1, make sure you have an AWS account, set your AWS Key Info in your shell and run "flintrock configure" to configure your cluster(Pls refer to https://heather.miller.am/blog/launching-a-spark-cluster-part-1.html#setting-up-flintrock-and-amazon-web-services)


workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
echo "worker0: ${workers[0]}"
echo "worker1: ${workers[1]}"

excecute_on_cluster(){
	command=$1
	eval $command >/dev/null  2>/home/ec2-user/nohup/master.log  &
	ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no $command >/dev/null  2>/home/ec2-user/nohup/worker0.log  &
	ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no $command >/dev/null  2>/home/ec2-user/nohup/worker1.log  &
	wait
}

configure_alluxio(){
	# configure alluxio
	echo "Configure alluxio"

	excecute_on_cluster 'cd /home/ec2-user; cp /home/ec2-user/alluxio/conf/alluxio-site.properties.template /home/ec2-user/alluxio/conf/alluxio-site.properties'

	excecute_on_cluster 'echo "alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	
	excecute_on_cluster 'echo "alluxio.user.file.delete.unchecked=true" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	
	excecute_on_cluster 'echo "alluxio.user.file.passive.cache.enabled=false" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'

	excecute_on_cluster 'echo "alluxio.master.hostname=$(cat /home/ec2-user/hadoop/conf/masters)" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;echo "alluxio.underfs.address=hdfs://$(cat /home/ec2-user/hadoop/conf/masters):9000/alluxio/root/" >> /home/ec2-user/alluxio/conf/alluxio-site.properties'


	excecute_on_cluster 'hadoop fs -mkdir -p /alluxio/root/'

	excecute_on_cluster 'cp /home/ec2-user/hadoop/conf/masters /home/ec2-user/alluxio/conf/masters;
	cp /home/ec2-user/hadoop/conf/slaves /home/ec2-user/alluxio/conf/workers'

}
launch() {

	# echo "delete java1.8 installed by flintrock"
	del_jdk="sudo yum -y remove java-1.8.0-openjdk.x86_64 java-1.8.0-openjdk-headless.x86_64"
	excecute_on_cluster "$del_jdk"

	# echo "delete JAVA_HOME set by flintrock"
	del_javahome='echo `sed -e '/JAVA_HOME/d' /etc/environment` | sudo tee /etc/environment; source /etc/environment'
	excecute_on_cluster "$del_javahome"


	# download Oracle JDK 1.8
	dl_oracle_jdk='wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" https://download.oracle.com/otn-pub/java/jdk/8u201-b09/42970487e3af4f5aa5bca3f542482c60/jdk-8u201-linux-x64.tar.gz;tar zxvf jdk-8u201-linux-x64.tar.gz;rm jdk-8u201-linux-x64.tar.gz'
	excecute_on_cluster "$dl_oracle_jdk"

	# set Env Variable for Oracle JDK 1.8
	set_env_var='echo "export JAVA_HOME=\$HOME/jdk1.8.0_201" >> /home/ec2-user/.bashrc; echo "export JRE_HOME=\$JAVA_HOME/jre" >> /home/ec2-user/.bashrc;	echo "export CLASSPATH=.:\$JAVA_HOME/lib:\$JRE_HOME/lib" >> /home/ec2-user/.bashrc; echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /home/ec2-user/.bashrc'
	excecute_on_cluster "$set_env_var"

	export JAVA_HOME=$HOME/jdk1.8.0_201
	export JRE_HOME=$JAVA_HOME/jre
	export CLASSPATH=.:$JAVA_HOME/lib:$JRE_HOME/lib
	export PATH=$JAVA_HOME/bin:$PATH

	# Register Oracle JDK 1.8
	echo "Register Oracle JDK 1.8"

	reg_jdk='sudo update-alternatives --install /usr/bin/java java /home/ec2-user/jdk1.8.0_201/bin/java 300;sudo update-alternatives --install /usr/bin/javac javac /home/ec2-user/jdk1.8.0_201/bin/javac 300;sudo update-alternatives --install /usr/bin/jar jar /home/ec2-user/jdk1.8.0_201/bin/jar 300;sudo update-alternatives --install /usr/bin/javah javah /home/ec2-user/jdk1.8.0_201/bin/javah 300;sudo update-alternatives --install /usr/bin/javap javap /home/ec2-user/jdk1.8.0_201/bin/javap 300;'
	excecute_on_cluster "$reg_jdk"

	# Install maven
	echo "Install maven"

	mvn1='wget http://mirrors.ocf.berkeley.edu/apache/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz; tar zxvf apache-maven-3.5.4-bin.tar.gz; rm apache-maven-3.5.4-bin.tar.gz'
	excecute_on_cluster "$mvn1"

	mvn2='echo "export MAVEN_HOME=\$HOME/apache-maven-3.5.4" >> /home/ec2-user/.bashrc; echo "export PATH=\$MAVEN_HOME/bin:\$PATH" >> /home/ec2-user/.bashrc'
	excecute_on_cluster "$mvn2"

	export MAVEN_HOME=$HOME/apache-maven-3.5.4
	export PATH=$MAVEN_HOME/bin:$PATH

	
	# Install git & setup
	echo "Install git"

	Git='sudo yum -y install git;mkdir -p /home/ec2-user/logs'
	excecute_on_cluster "$Git"
	
	echo "Install tools for master"

	curl https://bintray.com/sbt/rpm/rpm > bintray-sbt-rpm.repo
	sudo mv bintray-sbt-rpm.repo /etc/yum.repos.d/
	sudo yum -y install python-pip gcc make flex bison byacc sbt
	sudo pip install click

	echo "export PYTHONPATH=\$SPARK_HOME/python:\$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:\$PYTHONPATH" >> /home/ec2-user/.bashrc

	export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip

	# Download alluxio source code
	echo "Download & compile alluxio"

	excecute_on_cluster 'git clone git://github.com/CheneyYu96/alluxio.git'

	# Compile alluxio source code
	excecute_on_cluster 'source /home/ec2-user/.bashrc; cd /home/ec2-user/alluxio; mvn install -Phadoop-2 -Dhadoop.version=2.8.5 -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true'

	# Download workload & compile
	echo "Download & compile workload"

	git clone git://github.com/CheneyYu96/SparkSQL-test.git
	
	mv /home/ec2-user/SparkSQL-test /home/ec2-user/tpch-spark
	cd /home/ec2-user/tpch-spark/dbgen
	make

	configure_alluxio

	# configure spark
	echo "Configure spark"

	excecute_on_cluster 'cp /home/ec2-user/spark/conf/spark-defaults.conf.template /home/ec2-user/spark/conf/spark-defaults.conf'
	excecute_on_cluster 'cp /home/ec2-user/spark/conf/log4j.properties.template /home/ec2-user/spark/conf/log4j.properties'

	#flintrock run-command $cluster_name 'echo "spark.driver.extraClassPath /home/ec2-user/alluxio/client/alluxio-1.8.1-client.jar" >> /home/ec2-user/spark/conf/spark-defaults.conf;
	#echo "spark.executor.extraClassPath /home/ec2-user/alluxio/client/alluxio-1.8.1-client.jar" >> /home/ec2-user/spark/conf/spark-defaults.conf'
	
	excecute_on_cluster 'echo "spark.driver.extraClassPath /home/ec2-user/alluxio/client/$(ls /home/ec2-user/alluxio/client)" >> /home/ec2-user/spark/conf/spark-defaults.conf; echo "spark.executor.extraClassPath /home/ec2-user/alluxio/client/$(ls /home/ec2-user/alluxio/client)" >> /home/ec2-user/spark/conf/spark-defaults.conf'

	# set hadoop
	echo "Configure hadoop"

	excecute_on_cluster 'sed -i "/<\/configuration>/d" /home/ec2-user/hadoop/conf/core-site.xml; echo "
	  <property>
	    <name>fs.alluxio.impl</name>
	    <value>alluxio.hadoop.FileSystem</value>
	  </property>
	</configuration>" >> /home/ec2-user/hadoop/conf/core-site.xml
	'

	excecute_on_cluster 'echo "export HADOOP_CLASSPATH=/home/ec2-user/alluxio/client/$(ls /home/ec2-user/alluxio/client):\$HADOOP_CLASSPATH" >> /home/ec2-user/.bashrc; source /home/ec2-user/.bashrc'
	
	export HADOOP_CLASSPATH=/home/ec2-user/alluxio/client/$(ls /home/ec2-user/alluxio/client):$HADOOP_CLASSPATH

	echo "setup wondershaper"
	excecute_on_cluster "git clone https://github.com/magnific0/wondershaper.git;sudo yum install -y tc;cd wondershaper;sudo make install;sudo systemctl enable wondershaper.service;sudo systemctl start wondershaper.service;"

	# restart 
	echo "Restart"

	/home/ec2-user/alluxio/bin/alluxio-stop.sh all
	/home/ec2-user/hadoop/sbin/stop-dfs.sh
	/home/ec2-user/hadoop/sbin/start-dfs.sh
	/home/ec2-user/alluxio/bin/alluxio format
	/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount

}

start() {
	echo "Start cluster"

	echo "Configure alluxio"
	excecute_on_cluster 'cp /home/ec2-user/hadoop/conf/masters /home/ec2-user/alluxio/conf/masters; cp /home/ec2-user/hadoop/conf/slaves /home/ec2-user/alluxio/conf/workers'

	excecute_on_cluster 'sed -i "\$d" /home/ec2-user/alluxio/conf/alluxio-site.properties;sed -i "\$d"  /home/ec2-user/alluxio/conf/alluxio-site.properties;'

	excecute_on_cluster 'echo "alluxio.master.hostname=$(cat /home/ec2-user/hadoop/conf/masters)" >> /home/ec2-user/alluxio/conf/alluxio-site.properties; echo "alluxio.underfs.address=hdfs://$(cat /home/ec2-user/hadoop/conf/masters):9000/alluxio/root/" >> /home/ec2-user/alluxio/conf/alluxio-site.properties'

	echo "Restart alluxio & hdfs"
	/home/ec2-user/alluxio/bin/alluxio-stop.sh all
	/home/ec2-user/hadoop/sbin/stop-dfs.sh
	/home/ec2-user/hadoop/sbin/start-dfs.sh
	/home/ec2-user/alluxio/bin/alluxio format
	/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount

}

# stop() {
# 	cluster_name=$1
# 	echo "Stop cluster ${cluster_name}"

# 	flintrock stop $cluster_name
# }

# destroy() {
# 	cluster_name=$1
# 	echo "Destory cluster ${cluster_name}"

# 	flintrock destroy $cluster_name
# }

updata_alluxio() {
	echo "Update & Recompile alluxio for cluster"

	echo "Pull alluxio for repo"
	excecute_on_cluster 'cd /home/ec2-user/alluxio; git checkout conf/threshold; git pull'

	echo "Compile ... "
	excecute_on_cluster 'cd /home/ec2-user/alluxio; mvn install -Phadoop-2 -Dhadoop.version=2.8.5 -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true'

	configure_alluxio $cluster_name
	
	echo "Restart alluxio & hdfs"
	/home/ec2-user/alluxio/bin/alluxio-stop.sh all
	/home/ec2-user/hadoop/sbin/stop-dfs.sh
	/home/ec2-user/hadoop/sbin/start-dfs.sh
	/home/ec2-user/alluxio/bin/alluxio format
	/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount

}

usage() {
    echo "Usage: $0 start|stop|launch|destroy|update|conf_alluxio <cluster name>"
}

if [[ "$#" -lt 1 ]]; then
    usage
    exit 1
else
    case $1 in
        start)                  start
                                ;;
        stop)                   stop
                                ;;
        launch)                	launch
                                ;;
        destroy)				destroy
        						;;
        update)					updata_alluxio
        						;;
		conf_alluxio)			configure_alluxio
								;;             
        * )                     usage
    esac
fi


