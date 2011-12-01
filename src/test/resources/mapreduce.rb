def map(rowKey, columns)
   result = {}
   columns.each do |column_name, value|
      result[column_name] = (value.to_i*2).to_s
   end
   return result
end

def reduce(key, values)
	rows = {}
	total = 0
	values.each do |value|
	  total += 1
	  columns = {}
	  (0..total).each do |i|
	 	columns["col#{i}"] = i.to_s
	  end
	  rowKey = "#{key}-#{value}"
	  rows[rowKey] = columns
	end
	return rows
end
