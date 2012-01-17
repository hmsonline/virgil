curl -X POST http://localhost:8080/virgil/job?jobName=wordcount\&inputKeyspace=dummy\&inputColumnFamily=book\&outputKeyspace=stats\&outputColumnFamily=word_counts --data-binary @wordcount.rb
