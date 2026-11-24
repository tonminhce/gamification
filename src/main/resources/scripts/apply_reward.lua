local leaderboardType = redis.call('TYPE', KEYS[2])['ok']
if leaderboardType ~= 'none' and leaderboardType ~= 'zset' then
    return redis.error_reply('leaderboard key has unexpected type: ' .. leaderboardType)
end

local processedType = redis.call('TYPE', KEYS[1])['ok']
if processedType ~= 'none' and processedType ~= 'string' then
    return redis.error_reply('processed invoice key has unexpected type: ' .. processedType)
end

local companyId = ARGV[2]
local points = tonumber(ARGV[3])
if not points or points <= 0 then
    return redis.error_reply('points must be a positive number')
end

local markerValue = redis.call('GET', KEYS[1])
local marker = nil
if markerValue then
    local ok, decoded = pcall(cjson.decode, markerValue)
    if not ok or type(decoded) ~= 'table' then
        return redis.error_reply('processed invoice marker is malformed')
    end
    marker = decoded
end

if marker and marker.state == 'APPLIED' then
    return 0
end

if marker then
    if marker.state ~= 'PENDING'
        or marker.companyId ~= companyId
        or tonumber(marker.points) ~= points
        or not tonumber(marker.scoreBefore) then
        return redis.error_reply('processed invoice marker has unexpected state')
    end
end

local scoreValue = redis.call('ZSCORE', KEYS[2], companyId)
local currentScore = scoreValue and tonumber(scoreValue) or 0

if marker then
    local scoreBefore = tonumber(marker.scoreBefore)
    local scoreAfter = scoreBefore + points
    if currentScore == scoreAfter then
        redis.call('SET', KEYS[1], cjson.encode({
            state = 'APPLIED',
            companyId = companyId,
            points = points
        }))
        return 1
    end

    if currentScore == scoreBefore then
        redis.call('ZINCRBY', KEYS[2], points, companyId)
        redis.call('SET', KEYS[1], cjson.encode({
            state = 'APPLIED',
            companyId = companyId,
            points = points
        }))
        return 1
    end

    return redis.error_reply('partial reward state requires reconciliation')
end

redis.call('SET', KEYS[1], cjson.encode({
    state = 'PENDING',
    companyId = companyId,
    points = points,
    scoreBefore = currentScore
}))
redis.call('ZINCRBY', KEYS[2], points, companyId)
redis.call('SET', KEYS[1], cjson.encode({
    state = 'APPLIED',
    companyId = companyId,
    points = points
}))
return 1
