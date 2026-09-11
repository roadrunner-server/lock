local key = KEYS[1]
local op = ARGV[1]
local id = ARGV[2]
local ttl = tonumber(ARGV[3])
local clock = redis.call('TIME')
local now = clock[1] * 1000000 + clock[2]

-- Scores use Redis server time in microseconds.
-- https://redis.io/docs/latest/commands/time/
-- The key expiry follows the largest score. An expired member can stay in the set.
-- Every operation removes the expired members first, so the script always writes.
redis.call('ZREMRANGEBYSCORE', key, '-inf', string.format('%.0f', now))

local function expireKey()
    local last = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
    if #last == 0 then
        return
    end
    if last[2] == 'inf' then
        redis.call('PERSIST', key)
    else
        redis.call('PEXPIREAT', key, math.ceil(tonumber(last[2]) / 1000))
    end
end

local function save(member)
    local expiry = '+inf'
    if ttl ~= 0 then
        expiry = string.format('%.0f', now + ttl)
    end
    redis.call('ZADD', key, expiry, member)
    expireKey()
end

if op == 'lock' or op == 'read' then
    local first = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local member = 'w:' .. id
    if op == 'read' then
        member = 'r:' .. id
        -- A read lock with the same ID blocks another read acquisition.
        -- The caller must not retry after expiry or notification.
        if redis.call('ZSCORE', key, member) then
            return {0, -2}
        end
        if #first == 0 or string.sub(first[1], 1, 2) == 'r:' then
            save(member)
            redis.call('PUBLISH', key, '')
            return {1, 0}
        end
    elseif #first == 0 then
        save(member)
        return {1, 0}
    elseif first[1] == 'r:' .. id and redis.call('ZCARD', key) == 1 then
        redis.call('ZREM', key, first[1])
        save(member)
        redis.call('PUBLISH', key, '')
        return {1, 0}
    end
    if first[2] == 'inf' then
        return {0, -1}
    end
    return {0, tonumber(first[2]) - now}
end

if op == 'force' then
    local removed = redis.call('DEL', key)
    if removed == 1 then
        redis.call('PUBLISH', key, '')
    end
    return {removed, 0}
end

if op == 'exists' and id == '*' then
    if redis.call('ZCARD', key) > 0 then
        return {1, 0}
    end
    return {0, 0}
end

local member = 'w:' .. id
if not redis.call('ZSCORE', key, member) then
    member = 'r:' .. id
    if not redis.call('ZSCORE', key, member) then
        return {0, 0}
    end
end

if op == 'exists' then
    return {1, 0}
elseif op == 'release' then
    redis.call('ZREM', key, member)
    expireKey()
elseif op == 'ttl' then
    save(member)
end
redis.call('PUBLISH', key, '')
return {1, 0}
