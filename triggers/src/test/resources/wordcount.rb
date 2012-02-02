require 'json'
require 'rest-client'

def map(rowKey, columns)
   result = [] 
   columns.each do |column_name, value|
      words = value.split 
      words.each do |word|
         result << [word, "1"]
      end
   end
   return result
end

def reduce(key, values)
	rows = {}
	total = 0
	columns = {}
	values.each do |value|
	  total += value.to_i
	end
	columns["count"] = total.to_s
	rows[key] = columns
	return rows
end
