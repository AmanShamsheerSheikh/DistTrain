local document_id = KEYS[1]
local chunk_id     = ARGV[1]
local limit        = tonumber(ARGV[2])

local added = redis.call('SADD', document_id, chunk_id)
if added == 0 then
    return 0
end

local count = redis.call('SCARD', document_id)
if count == 1 then
    redis.call('EXPIRE', document_id, 21600)
end
if count >= limit then
    return 1
end
return 0