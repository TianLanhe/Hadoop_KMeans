#!/usr/bin/ruby
require "./Sample"

# 读取质心文件
centers = []
File.open('kmeans_centers.data','r') do |file|
	file.each_line do |line|
		index, content = line.split(/\t/)
		centers[index.to_i] = Sample.new(content)
	end
end

# 计算点所属最近的质心，输出<质心编号，点>
$stdin.each_line do |line|
	sample = Sample.new(line)
	distances = centers.map { |center| Sample.get_dist(center,sample) }
	min = distances.index(distances.min)
	$stdout.puts "#{min}\t#{sample}"
end

