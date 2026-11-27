-- KEYS[1]: leaderboard ZSET
-- ARGV[1]: companyId
-- Returns nil if company has no entry, otherwise { scoreString, higherCountString }.

local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score then
    return nil
end
local higher = redis.call('ZCOUNT', KEYS[1], '(' .. score, '+inf')
return { tostring(score), tostring(higher) }
