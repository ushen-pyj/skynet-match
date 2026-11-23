local skynet = require "skynet"

local function limit(value, default, maximum)
    if value == nil then return default end
    local n = math.tointeger(tonumber(value))
    assert(n and n >= 1 and n <= maximum, "invalid matchmaker startup limit")
    return n
end

local capacity_arg, batch_arg, timeout_arg, receipts_arg = ...
local capacity = limit(capacity_arg, 4096, 1000000)
local batch = limit(batch_arg, math.min(capacity, 4096), math.min(capacity, 4096))
local timeout = limit(timeout_arg, 300, 6000) -- Skynet ticks
local receipt_limit = limit(receipts_arg, 64, 1024)
local ticket_bytes, result_bytes = 8192, 262144

-- Ring order is independent of the hash index. Only the wire ticket is serialized.
local pending, head, count = {}, nil, 0
local receipts, receipt_count = {}, 0
local running = false
local command = {}

local function valid_id(id)
    return type(id) == "string" and #id > 0 and #id <= 128
end

function command.enqueue(id, data)
    if not valid_id(id) then return nil, "invalid_id" end
    if pending[id] then return nil, "already_queued" end
    if count == capacity then return nil, "queue_full" end
    local ticket = { id = id, data = data }
    if #skynet.packstring(ticket) > ticket_bytes then return nil, "ticket_too_large" end
    local node = { ticket = ticket }
    if head then
        node.prev, node.next = head.prev, head
        head.prev.next, head.prev = node, node
    else
        head = node
        node.prev, node.next = node, node
    end
    pending[id] = node
    count = count + 1
    return true
end

local function remove(node)
    pending[node.ticket.id] = nil
    if count == 1 then
        head = nil
    else
        node.prev.next, node.next.prev = node.next, node.prev
        if node == head then head = node.next end
    end
    node.prev, node.next = nil, nil
    count = count - 1
end

function command.cancel(id)
    local node = pending[id]
    if not node then return false end
    remove(node)
    return true
end

local function snapshot()
    local tickets, by_id = {}, {}
    local node = head
    for i = 1, math.min(count, batch) do
        local ticket = node.ticket
        tickets[i], by_id[ticket.id] = ticket, ticket
        node = node.next
    end
    head = node
    return tickets, by_id
end

local function array_size(t, maximum)
    if type(t) ~= "table" then return nil end
    local n = #t
    if n > maximum then return nil end
    local count_keys = 0
    for k in pairs(t) do
        if math.type(k) ~= "integer" or k < 1 or k > n then return nil end
        count_keys = count_keys + 1
    end
    if count_keys == n then return n end
end

local function resolve(proposals, by_id, n)
    local remaining = n * 8 -- Allow alternative groupings, bound validation work.
    if not array_size(proposals, remaining) then return nil, "invalid_proposals" end
    local accepted, claimed, seen = {}, {}, {}
    for i, proposal in ipairs(proposals) do
        local ids = type(proposal) == "table" and proposal.ids
        local size = array_size(ids, math.min(n, remaining))
        if not size or size == 0 then return nil, "invalid_proposals" end
        remaining = remaining - size
        local eligible = true
        for _, id in ipairs(ids) do
            local original = by_id[id]
            if not original or seen[id] == i then return nil, "invalid_proposals" end
            seen[id] = i
            local node = pending[id]
            if not node or node.ticket ~= original or claimed[id] then eligible = false end
        end
        if eligible then
            accepted[#accepted + 1] = proposal
            for _, id in ipairs(ids) do claimed[id] = true end
        end
    end
    return accepted, claimed
end

local function evaluate(worker, profile, request_id)
    local tickets, by_id = snapshot()
    local proposals = {}
    if #tickets > 0 then
        local rpc = skynet.request(table.pack(worker, "lua", "run", profile, tickets))
        for _, response in rpc:select(timeout) do
            if not response then return nil, "run_failed" end
            proposals = response[1]
        end
        if rpc.timeout then return nil, "timeout" end
    end
    local accepted, claimed = resolve(proposals, by_id, #tickets)
    if not accepted then return nil, claimed end

    -- Prepare the exact reply before committing. No yielding below this point.
    local packed = skynet.packstring(accepted)
    if #packed > result_bytes then return nil, "result_too_large" end
    if request_id then
        receipts[request_id] = packed
        receipt_count = receipt_count + 1
    end
    for id in pairs(claimed) do remove(pending[id]) end
    return packed
end

local function run(worker, profile, request_id)
    if request_id ~= nil then
        if not valid_id(request_id) then return nil, "invalid_request_id" end
        local packed = receipts[request_id]
        if packed then return packed end
    end
    if running then return nil, "busy" end
    if request_id and receipt_count == receipt_limit then return nil, "receipts_full" end
    if #skynet.packstring(profile) > ticket_bytes then return nil, "profile_too_large" end
    running = true
    local ok, result, reason = pcall(evaluate, worker, profile, request_id)
    running = false
    if not ok then
        skynet.error("matchmaker run failed", result)
        return nil, "run_failed"
    end
    return result, reason
end

function command.ack(request_id)
    if not receipts[request_id] then return false end
    receipts[request_id] = nil
    receipt_count = receipt_count - 1
    return true
end

skynet.start(function()
    skynet.dispatch("lua", function(_, _, op, ...)
        if op == "run" then
            local packed, err = run(...)
            if packed then
                skynet.ret(packed)
            else
                skynet.retpack(nil, err)
            end
            return
        end
        local handler = command[op]
        if handler then
            skynet.retpack(handler(...))
        else
            skynet.retpack(nil, "unknown_command")
        end
    end)
end)

