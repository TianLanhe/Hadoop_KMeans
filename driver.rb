#!/usr/bin/ruby
require "./Sample"

ITERATION = 0.1
GET_COMMAND = 'hdfs dfs -get output/part-00000'
RM_COMMAND = 'hdfs dfs -rm -r output'
EXEC_COMMAND = <<EOF
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar \
-files kmeans_centers.data \
-mapper map.rb \
-reducer reduce.rb \
-input input \
-output output
EOF

count = 0
loop do
	$stdout.puts RM_COMMAND
	system(RM_COMMAND)
	$stdout.puts EXEC_COMMAND
	system(EXEC_COMMAND)#进行一次mapreduce
	count += 1
	$stdout.puts "MapReduce: #{count}"
	$stdout.puts GET_COMMAND
	system(GET_COMMAND)#结果取回本地
	#与原质心文件对比,所有质心的距离都小于阈值则停止迭代
	break if File.readlines('part-00000').zip(File.readlines('kmeans_centers.data')).all? do |(new_line,old_line)|
		dist = Sample.get_dist(Sample.new(new_line.split(/\t/)[1]),Sample.new(old_line.split(/\t/)[1]))
		$stdout.puts "Distance: #{dist}"
		dist <= ITERATION
	 end
	File.rename('part-00000','kmeans_centers.data')#否则新质心文件取代旧质心文件，继续迭代
end
