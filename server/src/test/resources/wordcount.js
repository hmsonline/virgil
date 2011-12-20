function map(rowKey, columns) {
  print('Got here.');
  print(rowKey);
  print(columns.size());
  for (i in columns.entrySet()) {
	print('key is: ' + i + ', value is: ' + columns[i]);
  }

}
