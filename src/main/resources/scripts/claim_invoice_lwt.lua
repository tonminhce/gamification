-- Atomic CLAIMED -> EMITTED invoice dedupe state machine (single-key CAS with bounded TTL).
-- KEYS[1]: einv:dedupe-ledger:{invoiceId}
-- ARGV[1]: action ('CLAIM' | 'EMIT')
-- ARGV[2]: companyId
-- ARGV[3]: sourceId (e.g., partition:offset or claim token)
-- ARGV[4]: nowMs
-- ARGV[5]: ttlMs
-- ARGV[6]: claimLeaseMs
--
-- Return array: { statusCode, existingCompanyId }
--   1 = CLAIMED_NEW
--   2 = DUPLICATE_EMITTED
--   3 = RECOVERABLE_CLAIMED_UNEMITTED
--   4 = CONFLICT
--   5 = MARKED_EMITTED
--   6 = IN_FLIGHT_CLAIMED_BY_OTHER

local action = ARGV[1]
local companyId = ARGV[2]
local sourceId = ARGV[3]
local nowMs = tonumber(ARGV[4])
local ttlMs = tonumber(ARGV[5])
local leaseMs = tonumber(ARGV[6])

if action == 'EMIT' then
    local existingSource = redis.call('HGET', KEYS[1], 'sourceId')
    local existingCompany = redis.call('HGET', KEYS[1], 'companyId') or ''
    if existingSource == sourceId then
        redis.call('HSET', KEYS[1], 'state', 'EMITTED', 'emittedAtMs', tostring(nowMs))
        return { 5, existingCompany }
    end
    local currentState = redis.call('HGET', KEYS[1], 'state') or ''
    return { currentState == 'EMITTED' and 2 or 6, existingCompany }
end

if action ~= 'CLAIM' then
    return redis.error_reply('unsupported ledger action')
end

if not ttlMs or ttlMs <= 0 then
    return redis.error_reply('ledger ttlMs must be positive')
end

local fields = redis.call('HMGET', KEYS[1], 'state', 'companyId', 'sourceId', 'claimedAtMs')
local state = fields[1]
local storedCompany = fields[2]
local storedSource = fields[3]
local claimedAtMs = tonumber(fields[4]) or 0

if not state then
    redis.call('HSET', KEYS[1],
        'state', 'CLAIMED',
        'companyId', companyId,
        'sourceId', sourceId,
        'claimedAtMs', tostring(nowMs))
    redis.call('PEXPIRE', KEYS[1], ttlMs)
    return { 1, companyId }
end

if storedCompany ~= companyId then
    return { 4, storedCompany or '' }
end

if state == 'EMITTED' then
    return { 2, storedCompany }
end

-- Existing record is still CLAIMED (unemitted). Same redelivery source or expired lease recovers the claim.
if storedSource == sourceId or (nowMs - claimedAtMs) >= leaseMs then
    redis.call('HSET', KEYS[1],
        'sourceId', sourceId,
        'claimedAtMs', tostring(nowMs))
    return { 3, storedCompany }
end

return { 6, storedCompany }
