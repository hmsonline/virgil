# Releases Virgil
mvn clean install 
mvn hadoop:pack -f mapreduce/pom.xml -Dmaven.test.skip=true
rm -rf release/assembly/mapreduce/jars
mkdir release/assembly/mapreduce/jars
cp mapreduce/target/hadoop-deploy/*.jar release/assembly/mapreduce/jars/.
mvn assembly:assembly -f release/pom.xml -Dmaven.test.skip=true
