-- wrk benchmark for the stats-cruncher demo dataset.
--
-- Shapes (25% each):
--   1. random time window only
--   2. random time window + country filter
--   3. two disjoint random windows
--   4. two disjoint random windows + event_type filter
--
-- Window width: random between 1 month and the full dataset span (~2 years).
--
-- Usage:
--   wrk -t4 -c4 -d30s --timeout 5s --latency -s demo/bench.lua http://localhost:8080

math.randomseed(os.time())

local COUNTRIES   = {"DE", "US", "FR", "PL", "GB", "JP", "BR", "CA", "AU", "NL"}
local EVENT_TYPES = {"purchase", "refund"}

-- dataset spans 2024-01-01T00:00:00Z to 2026-01-01T00:00:00Z
local DATA_START = 1704067200  -- 2024-01-01T00:00:00Z
local DATA_END   = 1767225600  -- 2026-01-01T00:00:00Z

local MIN_WINDOW = 30  * 86400                  -- ~1 month
local MAX_WINDOW = DATA_END - DATA_START        -- full span

local function rand_window()
    local w  = math.random(MIN_WINDOW, MAX_WINDOW)
    local lo = math.random(DATA_START, DATA_END - MIN_WINDOW)
    local hi = math.min(lo + w, DATA_END)
    return lo, hi
end

local function fmt(ts)
    return os.date("!%Y-%m-%dT%H:%M:%SZ", ts)
end

local function rand_pick(t, n)
    local pool = {unpack(t)}
    for i = #pool, 2, -1 do
        local j = math.random(i)
        pool[i], pool[j] = pool[j], pool[i]
    end
    local out = {}
    for i = 1, math.min(n, #pool) do
        out[i] = '"' .. pool[i] .. '"'
    end
    return table.concat(out, ",")
end

local function rand_two_windows()
    local lo1, hi1 = rand_window()
    local gap = math.random(MIN_WINDOW, MIN_WINDOW * 3)
    local lo2 = hi1 + gap
    local hi2 = math.min(lo2 + math.random(MIN_WINDOW, MAX_WINDOW), DATA_END)
    if lo2 >= DATA_END then
        lo2 = DATA_START
        hi2 = math.min(lo2 + MIN_WINDOW * 2, DATA_END)
    end
    return string.format(
        '[{"gte":"%s","lt":"%s"},{"gte":"%s","lt":"%s"}]',
        fmt(lo1), fmt(hi1), fmt(lo2), fmt(hi2))
end

local function build_body()
    local shape = math.random(4)
    local lo, hi = rand_window()

    if shape == 1 then
        return string.format(
            '{"ranges":{"occurred_at":{"gte":"%s","lt":"%s"}}}',
            fmt(lo), fmt(hi))
    elseif shape == 2 then
        return string.format(
            '{"must":{"country":[%s]},"ranges":{"occurred_at":{"gte":"%s","lt":"%s"}}}',
            rand_pick(COUNTRIES, math.random(1, 3)), fmt(lo), fmt(hi))
    elseif shape == 3 then
        return string.format(
            '{"ranges":{"occurred_at":%s}}',
            rand_two_windows())
    else
        return string.format(
            '{"must":{"event_type":[%s]},"ranges":{"occurred_at":%s}}',
            rand_pick(EVENT_TYPES, 1), rand_two_windows())
    end
end

local request_headers = {["Content-Type"] = "application/json"}

request = function()
    return wrk.format("POST", "/query", request_headers, build_body())
end

local errors = 0

response = function(status, headers, body)
    if status ~= 200 then
        errors = errors + 1
    end
end

done = function(summary, latency, requests)
    io.write(string.format("\nNon-2xx responses: %d\n", errors))
end
