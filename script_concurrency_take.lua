-- Maintain a hash of request ids and their expiration times.
local rate_limit_key = KEYS[1]
local request_id = ARGV[1]
local limit = tonumber(ARGV[2])
local max_request_time_seconds = tonumber(ARGV[3])

local now = redis.call("TIME")[1]

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
                count++
		    end
		end
	end
	return count
end

local count = hmcountandfilter(rate_limit_key)
if count >= limit then
  return {false, count}
end

redis.call("HSET", rate_limit_key, request_id, now + max_request_time_seconds)
redis.call("EXPIRE", rate_limit_key, 5 * max_request_time_seconds)
return {true, count + 1}
