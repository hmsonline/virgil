# Releases Virgil
mvn clean install hadoop:pack -f mapreduce/pom.xml -Dmaven.test.skip=true
cp mapreduce/target/hadoop-deploy/*.jar server/mapreduce/jars
mvn clean install assembly:assembly -f server/pom.xml -Dmaven.test.skip=true
