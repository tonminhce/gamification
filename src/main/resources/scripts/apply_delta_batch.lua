local markerTtl = tonumber(ARGV[1])
if not markerTtl or markerTtl <= 0 then
    return redis.error_reply('marker TTL must be positive')
end

local function applyBatch(batch)
    for startIndex = 1, #batch.deltaIds, 1000 do
        local chunk = {}
        for index = startIndex, math.min(startIndex + 999, #batch.deltaIds) do
            table.insert(chunk, batch.deltaIds[index])
        end
        redis.call('SADD', KEYS[3], unpack(chunk))
    end

    for _, company in ipairs(batch.companies) do
        local scoreValue = redis.call('ZSCORE', KEYS[1], company.companyId)
        local currentScore = scoreValue and tonumber(scoreValue) or 0
        local scoreBefore = tonumber(company.scoreBefore)
        local scoreAfter = scoreBefore + tonumber(company.deltaPoints)
        if currentScore == scoreBefore then
            redis.call('ZINCRBY', KEYS[1], company.deltaPoints, company.companyId)
        elseif currentScore ~= scoreAfter then
            return redis.error_reply('partial delta batch requires reconciliation')
        end
    end
end

local markerValue = redis.call('GET', KEYS[2])
if markerValue then
    local ok, decoded = pcall(cjson.decode, markerValue)
    if not ok or type(decoded) ~= 'table' or decoded.state ~= 'PENDING' then
        return redis.error_reply('delta marker is malformed')
    end
    applyBatch(decoded)
    redis.call('DEL', KEYS[2])
end

local seen = {}
local uniqueDeltas = {}
for index = 2, #ARGV, 3 do
    local deltaId = ARGV[index]
    if not seen[deltaId] then
        seen[deltaId] = true
        table.insert(uniqueDeltas, {
            deltaId = deltaId,
            companyId = ARGV[index + 1],
            points = tonumber(ARGV[index + 2])
        })
    end
end

local newDeltas = {}
for startIndex = 1, #uniqueDeltas, 1000 do
    local chunkIds = {}
    for index = startIndex, math.min(startIndex + 999, #uniqueDeltas) do
        table.insert(chunkIds, uniqueDeltas[index].deltaId)
    end
    local members = redis.call('SMISMEMBER', KEYS[3], unpack(chunkIds))
    for index, member in ipairs(members) do
        if member == 0 then
            table.insert(newDeltas, uniqueDeltas[startIndex + index - 1])
        end
    end
end

if #newDeltas == 0 then
    redis.call('DEL', KEYS[2])
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
        table.insert(companyOrder, delta.companyId)
    end
    company.deltaPoints = company.deltaPoints + delta.points
end

local companyList = {}
for _, companyId in ipairs(companyOrder) do
    table.insert(companyList, companies[companyId])
end
local deltaIds = {}
for _, delta in ipairs(newDeltas) do
    table.insert(deltaIds, delta.deltaId)
end

local batch = {
    state = 'PENDING',
    deltaIds = deltaIds,
    companies = companyList
}
redis.call('SET', KEYS[2], cjson.encode(batch), 'EX', markerTtl)
applyBatch(batch)
redis.call('DEL', KEYS[2])
return #newDeltas