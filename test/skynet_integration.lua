local skynet = require "skynet.manager"

if ... == "mmf" then
    skynet.start(function()
        skynet.dispatch("lua", function(_, _, op, profile, tickets)
            assert(op == "run")
            if profile == "idle" then
                skynet.retpack({})
                return
            end
            if profile == "pairs" then
                local groups = {}
                for i = 2, #tickets, 2 do
                    groups[#groups + 1] = {ids = {tickets[i - 1].id, tickets[i].id}}
                end
                skynet.retpack(groups)
                return
            end
            if profile == "slow" then skynet.sleep(30) end
            if type(profile) == "table" then
                if profile.proposals then
                    skynet.retpack(profile.proposals)
                    return
                end
                local id = tickets[1].id
                local result, err = skynet.call(profile.pool, "lua", "run", skynet.self())
                assert(result == nil and err == "busy")
                assert(skynet.call(profile.pool, "lua", "cancel", id))
                assert(skynet.call(profile.pool, "lua", "enqueue", id, "replacement"))
            end
            if profile == "oversized" then
                skynet.retpack({{ids = {tickets[1].id}, data = string.rep("x", 262145)}})
                return
            end
            skynet.retpack({{ids = {tickets[1].id}, data = tickets[1].data}})
        end)
    end)
    return
end

local function check_concurrent_load(mmf)
    local actor = skynet.newservice("matchmaker", 4096, 256)
    local workers, per_worker = 32, 128
    local active, cancelled, matched = workers, {}, {}
    local producer_error
    local payload = string.rep("x", 128)
    local started = skynet.hpc()
    for producer = 1, workers do
        skynet.fork(function()
            local ok, err = xpcall(function()
                for i = 1, per_worker do
                    local id = ("%d/%d"):format(producer, i)
                    assert(skynet.call(actor, "lua", "enqueue", id, payload))
                    if i % 2 == 0 and skynet.call(actor, "lua", "cancel", id) then
                        cancelled[id] = true
                    end
                end
            end, debug.traceback)
            if not ok then producer_error = err end
            active = active - 1
        end)
    end
    local rounds, samples = 0, {}
    repeat
        local t = skynet.hpc()
        local groups = assert(skynet.call(actor, "lua", "run", mmf, "pairs"))
        samples[#samples + 1] = (skynet.hpc() - t) / 1e6
        rounds = rounds + 1
        for _, group in ipairs(groups) do
            for _, id in ipairs(group.ids) do
                assert(not matched[id], "ticket consumed twice: " .. id)
                matched[id] = true
            end
        end
        assert(not producer_error, producer_error)
    until active == 0 and skynet.call(actor, "lua", "stats").queued < 2
    local elapsed = (skynet.hpc() - started) / 1e9
    local state = skynet.call(actor, "lua", "stats")
    local nm, nc, left = 0, 0, 0
    for p = 1, workers do
        for i = 1, per_worker do
            local id = ("%d/%d"):format(p, i)
            if matched[id] then
                assert(not cancelled[id], "cancelled ticket consumed: " .. id)
                nm = nm + 1
            elseif cancelled[id] then
                nc = nc + 1
            else
                assert(skynet.call(actor, "lua", "cancel", id), "missing ticket: " .. id)
                left = left + 1
            end
        end
    end
    assert(nm == 2 * state.matched and left == state.queued and state.failures == 0)
    table.sort(samples)
    print(("mixed tickets=%d matched=%d cancelled=%d left=%d rounds=%d tickets/s=%.0f run_p99=%.3fms"):format(
        workers * per_worker, nm, nc, left, rounds, workers * per_worker / elapsed,
        samples[math.ceil(rounds * 0.99)]))
    skynet.kill(actor)
end

local function check()
    local mmf = skynet.newservice("skynet_integration", "mmf")
    local pool = skynet.newservice("matchmaker", 4, 2, 10, 1)
    local function call(op, ...)
        return skynet.call(pool, "lua", op, ...)
    end
    assert(call("enqueue", "a", {version = 1}))
    local result, err = call("run", mmf, "slow", "lost")
    assert(not result and err == "timeout")
    assert(call("cancel", "a"))
    assert(call("enqueue", "a", {version = 2}))
    result = assert(call("run", mmf, nil, "success"))
    assert(result[1].data.version == 2)
    assert(call("enqueue", "b"))
    skynet.sleep(35) -- A's old RPC has now had time to respond.
    assert(call("run", mmf, nil, "success")[1].ids[1] == "a")
    assert(call("stats").queued == 1)
    assert(call("ack", "success"))
    assert(call("run", mmf, nil, "next")[1].ids[1] == "b")
    assert(call("enqueue", "race"))
    assert(#call("run", mmf, {pool = pool}) == 0)
    assert(call("run", mmf)[1].data == "replacement")
    assert(call("ack", "next"))
    local function expect_error(code, op, ...)
        local value, reason = call(op, ...)
        assert(value == nil and reason == code, tostring(reason))
    end
    assert(#call("run", mmf) == 0)
    expect_error("invalid_id", "enqueue", "")
    expect_error("ticket_too_large", "enqueue", "big", string.rep("x", 8193))
    for _, id in ipairs({"a", "b", "c", "d"}) do assert(call("enqueue", id)) end
    expect_error("already_queued", "enqueue", "a")
    expect_error("queue_full", "enqueue", "e")
    expect_error("profile_too_large", "run", mmf, string.rep("x", 8193))
    -- The two-ticket window advances after every run, including failed rounds.
    for i, invalid in ipairs({{}, {"c", "c"}, {"unknown"}, {[2] = "c"}}) do
        local first = i % 2 == 1 and "a" or "c"
        expect_error("invalid_proposals", "run", mmf, {proposals = {{ids = {first}}, {ids = invalid}}})
        assert(call("stats").queued == 4)
    end
    expect_error("result_too_large", "run", mmf, "oversized", "retry")
    assert(call("stats").queued == 4 and call("stats").receipts == 0)
    local groups = assert(call("run", mmf, {proposals = {{ids = {"c", "d"}}, {ids = {"d"}}}}, "retry"))
    assert(#groups == 1 and #groups[1].ids == 2 and call("stats").queued == 2)
    expect_error("receipts_full", "run", mmf, nil, "another")
    assert(call("ack", "retry") and not call("ack", "retry"))
    local stopped = skynet.newservice("skynet_integration", "mmf")
    skynet.kill(stopped)
    expect_error("run_failed", "run", stopped)
    skynet.kill(pool)

    for _, batch in ipairs({4096, 256}) do
        local actor = skynet.newservice("matchmaker", 4096, batch)
        local payload = string.rep("x", 128)
        for i = 1, 4096 do
            assert(skynet.call(actor, "lua", "enqueue", tostring(i), payload))
        end
        local samples, total_ms = {}, 0
        for i = 1, 220 do
            local started = skynet.hpc()
            assert(#assert(skynet.call(actor, "lua", "run", mmf, "idle")) == 0)
            if i > 20 then
                local elapsed = (skynet.hpc() - started) / 1e6
                samples[#samples + 1] = elapsed
                total_ms = total_ms + elapsed
            end
        end
        table.sort(samples)
        print(("batch=%d samples=200 mean=%.3fms p50=%.3fms p99=%.3fms"):format(
            batch, total_ms / #samples, samples[100], samples[198]))
        skynet.kill(actor)
    end
    check_concurrent_load(mmf)
end

skynet.start(function()
    local ok, err = xpcall(check, debug.traceback)
    if ok then
        print("matchmaker integration passed")
        io.stdout:flush()
    else
        io.stderr:write(err, "\n")
        io.stderr:flush()
        os.exit(1)
    end
    skynet.abort()
end)

