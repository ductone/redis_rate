-- Heartbeat to extend the expiration time for a request ID.
local rate_limit_key = KEYS[1]
local request_id = ARGV[1]
local max_request_time_seconds = tonumber(ARGV[2])

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

-- Check if the request ID exists in the hash
local exists = redis.call("HEXISTS", rate_limit_key, request_id)
if exists == 0 then
  return {0, 0} -- Request ID not found
end

-- Update the expiration time for the request ID
redis.call("HSET", rate_limit_key, request_id, now + max_request_time_seconds)
redis.call("EXPIRE", rate_limit_key, 5 * max_request_time_seconds)

-- Get the current count of active requests
local count = 0
local bulk = redis.call('HGETALL', rate_limit_key)
local nextkey
for i, v in ipairs(bulk) do
  if i % 2 == 1 then
    nextkey = v
  else
    if tonumber(v) >= now then
      count = count + 1
    end
  end
end

return {1, count} -- Success, return the current count