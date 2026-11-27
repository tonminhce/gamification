local points = tonumber(ARGV[1])
local markerTtl = tonumber(ARGV[2])
if not points or points <= 0 or not markerTtl or markerTtl <= 0 then
    return redis.error_reply('points and marker TTL must be positive numbers')
end

local function applyBatch(batch)
    for startIndex = 1, #batch.invoiceIds, 1000 do
        local chunk = {}
        for index = startIndex, math.min(startIndex + 999, #batch.invoiceIds) do
            table.insert(chunk, batch.invoiceIds[index])
        end
        redis.call('SADD', KEYS[3], unpack(chunk))
    end

    for _, company in ipairs(batch.companies) do
        local scoreValue = redis.call('ZSCORE', KEYS[1], company.companyId)
        local currentScore = scoreValue and tonumber(scoreValue) or 0
        local scoreBefore = tonumber(company.scoreBefore)
        local scoreAfter = scoreBefore + tonumber(company.delta)
        if currentScore == scoreBefore then
            redis.call('ZINCRBY', KEYS[1], company.delta, company.companyId)
        elseif currentScore ~= scoreAfter then
            return redis.error_reply('partial reward batch requires reconciliation')
        end
    end
end

local batchValue = redis.call('GET', KEYS[2])
if batchValue == 'A' then
    -- No pending work; the current retry can still contain new records.
elseif batchValue then
    local ok, decoded = pcall(cjson.decode, batchValue)
    if not ok or type(decoded) ~= 'table' or decoded.state ~= 'PENDING' then
        return redis.error_reply('reward batch marker is malformed')
    end
    applyBatch(decoded)
    redis.call('SET', KEYS[2], 'A', 'EX', markerTtl)
end

local seen = {}
local uniqueIds = {}
local invoiceCompany = {}

for index = 3, #ARGV, 2 do
    local invoiceId = ARGV[index]
    if not seen[invoiceId] then
        seen[invoiceId] = true
        table.insert(uniqueIds, invoiceId)
        invoiceCompany[invoiceId] = ARGV[index + 1]
    end
end

local newInvoiceIds = {}
for startIndex = 1, #uniqueIds, 1000 do
    local chunk = {}
    for index = startIndex, math.min(startIndex + 999, #uniqueIds) do
        table.insert(chunk, uniqueIds[index])
    end
    local members = redis.call('SMISMEMBER', KEYS[3], unpack(chunk))
    for index, member in ipairs(members) do
        if member == 0 then
            table.insert(newInvoiceIds, chunk[index])
        end
    end
end

if #newInvoiceIds == 0 then
    redis.call('SET', KEYS[2], 'A', 'EX', markerTtl)
    return 0
end

local companies = {}
local companyOrder = {}
for _, invoiceId in ipairs(newInvoiceIds) do
    local companyId = invoiceCompany[invoiceId]
    local company = companies[companyId]
    if not company then
        local scoreValue = redis.call('ZSCORE', KEYS[1], companyId)
        company = {
            companyId = companyId,
            delta = 0,
            scoreBefore = scoreValue and tonumber(scoreValue) or 0
        }
        companies[companyId] = company
        table.insert(companyOrder, companyId)
    end
    company.delta = company.delta + points
end

local companyList = {}
for _, companyId in ipairs(companyOrder) do
    table.insert(companyList, companies[companyId])
end

local batch = {
    state = 'PENDING',
    invoiceIds = newInvoiceIds,
    companies = companyList
}
redis.call('SET', KEYS[2], cjson.encode(batch), 'EX', markerTtl)
applyBatch(batch)
redis.call('SET', KEYS[2], 'A', 'EX', markerTtl)
return #newInvoiceIds