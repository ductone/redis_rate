-- Maintain a hash of request ids and their expiration times.
local rate_limit_key = KEYS[1]
local request_id = ARGV[1]
local limit = tonumber(ARGV[2])
local max_request_time_seconds = tonumber(ARGV[3])

-- redis returns time as an array containing two integers: seconds of the epoch
-- time (10 digits) and microseconds (6 digits). for convenience we need to
-- convert them to a floating point number. the resulting number is 16 digits,
-- bordering on the limits of a 64-bit double-precision floating point number.
-- adjust the epoch to be relative to Jan 1, 2017 00:00:00 GMT to avoid floating
-- point problems. this approach is good until "now" is 2,483,228,799 (Wed, 09
-- Sep 2048 01:46:39 GMT), when the adjusted value is 16 digits.
local jan_1_2017 = 1483228800
local now = redis.call("TIME")
now = (now[1] - jan_1_2017) + (now[2] / 1000000)

local hmcountandfilter = function (key)
    local count = 0
    local bulk = redis.call('HGETALL', key)
	local nextkey
	for i, v in ipairs(bulk) do
		if i % 2 == 1 then
			nextkey = v
		else
		    if tonumber(v) < now then
                redis.call("HDEL", rate_limit_key, nextkey)
            else
                count = count + 1
		    end
		end
	end
	return count
end

local count = hmcountandfilter(rate_limit_key)
if count >= limit then
  return {0, count}
end

redis.call("HSET", rate_limit_key, request_id, now + max_request_time_seconds)
redis.call("EXPIRE", rate_limit_key, 5 * max_request_time_seconds)
return {1, count + 1}
