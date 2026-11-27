-- KEYS[1]: leaderboard ZSET
-- KEYS[2]: per-partition PENDING marker key
-- KEYS[3]: per-partition offset checkpoint key
-- KEYS[4..3+bucketCount]: time-bucketed processed-delta Sets
--
-- ARGV[1]: markerTtlSeconds
-- ARGV[2]: bucketCount (B >= 1)
-- ARGV[3]: batchMaxOffset (-1 if offsets are not tracked)
-- ARGV[4 + i*5 ..]: (offset, deltaId, companyId, points, bucketIndex)

local markerTtl = tonumber(ARGV[1])
if not markerTtl or markerTtl <= 0 then
    return redis.error_reply('marker TTL must be positive')
end

local bucketCount = tonumber(ARGV[2])
if not bucketCount or bucketCount < 1 then
    return redis.error_reply('bucket count must be positive')
end

local batchMaxOffset = tonumber(ARGV[3]) or -1

local function applyBatch(batch, verifyBefore)
    for _, bucket in ipairs(batch.buckets) do
        local ids = bucket.ids
        for startIndex = 1, #ids, 1000 do
            local chunk = {}
            for index = startIndex, math.min(startIndex + 999, #ids) do
                chunk[#chunk + 1] = ids[index]
            end
            redis.call('SADD', bucket.key, unpack(chunk))
        end
        redis.call('EXPIRE', bucket.key, markerTtl)
    end

    for _, company in ipairs(batch.companies) do
        if verifyBefore then
            local scoreValue = redis.call('ZSCORE', KEYS[1], company.companyId)
            local currentScore = scoreValue and tonumber(scoreValue) or 0
            local scoreBefore = tonumber(company.scoreBefore)
            local scoreAfter = scoreBefore + tonumber(company.deltaPoints)
            if currentScore == scoreBefore then
                redis.call('ZINCRBY', KEYS[1], company.deltaPoints, company.companyId)
            elseif currentScore ~= scoreAfter then
                return redis.error_reply('partial delta batch requires reconciliation')
            end
        else
            redis.call('ZINCRBY', KEYS[1], company.deltaPoints, company.companyId)
        end
    end
end

local markerValue = redis.call('GET', KEYS[2])
if markerValue then
    local ok, decoded = pcall(cjson.decode, markerValue)
    if not ok or type(decoded) ~= 'table' or decoded.state ~= 'PENDING'
            or type(decoded.buckets) ~= 'table' or type(decoded.companies) ~= 'table' then
        return redis.error_reply('delta marker is malformed')
    end
    local recoveryErr = applyBatch(decoded, true)
    if recoveryErr then
        return recoveryErr
    end
    redis.call('DEL', KEYS[2])
end

local checkpointRaw = redis.call('GET', KEYS[3])
local lastAppliedOffset = checkpointRaw and tonumber(checkpointRaw) or nil

local seen = {}
local byBucket = {}
local hasCandidate = false

for index = 4, #ARGV, 5 do
    local offset = tonumber(ARGV[index])
    local deltaId = ARGV[index + 1]
    local skipByOffset = lastAppliedOffset ~= nil and offset ~= nil and offset >= 0 and offset <= lastAppliedOffset
    if not skipByOffset and not seen[deltaId] then
        seen[deltaId] = true
        local bucketIdx = tonumber(ARGV[index + 4])
        if not bucketIdx or bucketIdx < 1 or bucketIdx > bucketCount then
            return redis.error_reply('bucket index out of range')
        end
        local bucketList = byBucket[bucketIdx]
        if not bucketList then
            bucketList = {}
            byBucket[bucketIdx] = bucketList
        end
        bucketList[#bucketList + 1] = {
            deltaId = deltaId,
            companyId = ARGV[index + 2],
            points = tonumber(ARGV[index + 3]),
            bucketIdx = bucketIdx
        }
        hasCandidate = true
    end
end

if not hasCandidate then
    if batchMaxOffset >= 0 and (lastAppliedOffset == nil or batchMaxOffset > lastAppliedOffset) then
        redis.call('SET', KEYS[3], tostring(batchMaxOffset), 'EX', markerTtl)
    end
    return 0
end

local newDeltas = {}
local newIdsByBucket = {}

for bucketIdx = 1, bucketCount do
    local candidateList = byBucket[bucketIdx]
    if candidateList then
        local bucketKey = KEYS[3 + bucketIdx]
        for startIndex = 1, #candidateList, 1000 do
            local chunkIds = {}
            for index = startIndex, math.min(startIndex + 999, #candidateList) do
                chunkIds[#chunkIds + 1] = candidateList[index].deltaId
            end
            local members = redis.call('SMISMEMBER', bucketKey, unpack(chunkIds))
            for index, member in ipairs(members) do
                if member == 0 then
                    local delta = candidateList[startIndex + index - 1]
                    newDeltas[#newDeltas + 1] = delta
                    local ids = newIdsByBucket[bucketIdx]
                    if not ids then
                        ids = {}
                        newIdsByBucket[bucketIdx] = ids
                    end
                    ids[#ids + 1] = delta.deltaId
                end
            end
        end
    end
end

if #newDeltas == 0 then
    if batchMaxOffset >= 0 and (lastAppliedOffset == nil or batchMaxOffset > lastAppliedOffset) then
        redis.call('SET', KEYS[3], tostring(batchMaxOffset), 'EX', markerTtl)
    end
    return 0
end

local companies = {}
local companyOrder = {}
for _, delta in ipairs(newDeltas) do
    local company = companies[delta.companyId]
    if not company then
        local scoreValue = redis.call('ZSCORE', KEYS[1], delta.companyId)
        company = {
            companyId = delta.companyId,
            deltaPoints = 0,
            scoreBefore = scoreValue and tonumber(scoreValue) or 0
        }
        companies[delta.companyId] = company
        companyOrder[#companyOrder + 1] = delta.companyId
    end
    company.deltaPoints = company.deltaPoints + delta.points
end

local companyList = {}
for _, companyId in ipairs(companyOrder) do
    companyList[#companyList + 1] = companies[companyId]
end

local bucketPayloads = {}
for bucketIdx = 1, bucketCount do
    local ids = newIdsByBucket[bucketIdx]
    if ids and #ids > 0 then
        bucketPayloads[#bucketPayloads + 1] = {
            key = KEYS[3 + bucketIdx],
            ids = ids
        }
    end
end

local batch = {
    state = 'PENDING',
    buckets = bucketPayloads,
    companies = companyList
}
redis.call('SET', KEYS[2], cjson.encode(batch), 'EX', markerTtl)
local applyErr = applyBatch(batch, false)
if applyErr then
    return applyErr
end
if batchMaxOffset >= 0 and (lastAppliedOffset == nil or batchMaxOffset > lastAppliedOffset) then
    redis.call('SET', KEYS[3], tostring(batchMaxOffset), 'EX', markerTtl)
end
redis.call('DEL', KEYS[2])
return #newDeltas
