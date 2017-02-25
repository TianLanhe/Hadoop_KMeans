class Sample
	attr_reader :arr

	DIMENSION = 60

	def initialize(par = [])
		if par.is_a?(Array)
			@arr = par
		elsif par.is_a?(String)
			read(par)
		end
	end

	def to_s
		@arr.join(' ')
	end

	def clear
		@arr.clear
	end

	def size
		@arr.size
	end

	def Sample.get_dist(s1,s2)
		dist = 0.0
		(0...s1.size).each { |i| dist += (s1.arr[i] - s2.arr[i])**2 }
		Math.sqrt(dist)
	end

	def read(line)
		@arr = line.split(/\s+/).map(&:to_f)
	end

end