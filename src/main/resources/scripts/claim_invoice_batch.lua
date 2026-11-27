local markerTtl = tonumber(ARGV[1])
local batchUid = ARGV[2]
local points = tonumber(ARGV[3])
if not markerTtl or markerTtl <= 0 or not points or points <= 0 then
    return redis.error_reply('marker TTL and points must be positive numbers')
end

local markerValue = redis.call('GET', KEYS[1])
if markerValue then
    local ok, decoded = pcall(cjson.decode, markerValue)
    if not ok or type(decoded) ~= 'table' or decoded.state ~= 'PENDING' then
        return redis.error_reply('claim marker is malformed')
    end
    for startIndex = 1, #decoded.invoiceIds, 1000 do
        local chunk = {}
        for index = startIndex, math.min(startIndex + 999, #decoded.invoiceIds) do
            table.insert(chunk, decoded.invoiceIds[index])
        end
        redis.call('SADD', KEYS[2], unpack(chunk))
    end
    return cjson.encode({
        batchUid = decoded.batchUid,
        recovered = true,
        companies = decoded.companies
    })
end

local seen = {}
local uniqueIds = {}
local invoiceCompany = {}
for index = 4, #ARGV, 2 do
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
    local members = redis.call('SMISMEMBER', KEYS[2], unpack(chunk))
    for index, member in ipairs(members) do
        if member == 0 then
            table.insert(newInvoiceIds, chunk[index])
        end
    end
end

if #newInvoiceIds == 0 then
    redis.call('DEL', KEYS[1])
    return cjson.encode({batchUid = batchUid, recovered = false, companies = {}})
end

local companies = {}
local companyOrder = {}
for _, invoiceId in ipairs(newInvoiceIds) do
    local companyId = invoiceCompany[invoiceId]
    local company = companies[companyId]
    if not company then
        company = {companyId = companyId, deltaPoints = 0}
        companies[companyId] = company
        table.insert(companyOrder, companyId)
    end
    company.deltaPoints = company.deltaPoints + points
end

local companyList = {}
for _, companyId in ipairs(companyOrder) do
    table.insert(companyList, companies[companyId])
end
redis.call('SET', KEYS[1], cjson.encode({
    state = 'PENDING',
    batchUid = batchUid,
    invoiceIds = newInvoiceIds,
    companies = companyList
}), 'EX', markerTtl)

for startIndex = 1, #newInvoiceIds, 1000 do
    local chunk = {}
    for index = startIndex, math.min(startIndex + 999, #newInvoiceIds) do
        table.insert(chunk, newInvoiceIds[index])
    end
    redis.call('SADD', KEYS[2], unpack(chunk))
end

return cjson.encode({
    batchUid = batchUid,
    recovered = false,
    companies = companyList
})