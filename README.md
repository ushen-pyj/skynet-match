# skynet-match

## Usage

```lua
local skynet = require "skynet"

skynet.start(function()
    skynet.dispatch("lua", function(_, _, cmd, profile, tickets)
        assert(cmd == "run")
        local groups = {}
        for i = 2, #tickets, 2 do
            groups[#groups + 1] = { ids = {tickets[i - 1].id, tickets[i].id} }
        end
        skynet.retpack(groups)
    end)
end)
```

```lua
local pool = skynet.newservice("matchmaker")
local rule = skynet.newservice("pair_rule")

assert(skynet.call(pool, "lua", "enqueue", "player-1"))
assert(skynet.call(pool, "lua", "enqueue", "player-2"))

local groups = assert(skynet.call(pool, "lua", "run", rule))
print(table.concat(groups[1].ids, ", ")) -- player-1, player-2
```