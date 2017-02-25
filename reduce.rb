#!/usr/bin/ruby
require "./Sample"

hash = Hash.new { |hash, key|hash[key] = [] }
$stdin.each_line do |line|
	key, value = line.split(/\t/)
	hash[key].push(Sample.new(value))
end

hash.each do |key, arr|
	result = Array.new(60,0.0)
	arr.each do |sample|
		sample.arr.each_with_index { |ele, index| result[index] += ele }
	end
	result.map! { |ele| ele / arr.size }
	puts "#{key}\t#{Sample.new(result)}"
end
