# Create the keyspace for input
curl -X PUT http://localhost:8080/virgil/data/dummy/ 

# Create the column family for output
curl -X PUT http://localhost:8080/virgil/data/dummy/book

# Create the keyspace for output
curl -X PUT http://localhost:8080/virgil/data/stats/

# Create the column family for output
curl -X PUT http://localhost:8080/virgil/data/stats/word_counts

# Insert some rows
curl -X PUT http://localhost:8080/virgil/data/dummy/book/brian -d "{\"page1\":\"I like cold beverages\",\"page2\":\"I like dogs\",\"page3\":\"I like brew\",\"page4\":\"Sausages are good\"}"

curl -X PUT http://localhost:8080/virgil/data/dummy/book/lisa -d "{\"page1\":\"I like red wine\",\"page2\":\"I like strawberries\",\"page3\":\"I dislike brussel sprouts\",\"page4\":\"Life is good.\"}"

curl -X PUT http://localhost:8080/virgil/data/dummy/book/collin -d "{\"page1\":\"I like pepperoni\"}"

curl -X PUT http://localhost:8080/virgil/data/dummy/book/owen -d "{\"page1\":\"I like watermelon\"}"

curl -X PUT http://localhost:8080/virgil/data/dummy/book/maya -d "{\"page1\":\"dog food is good\"}"
