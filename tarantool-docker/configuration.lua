-- function for batch insertion
function_code = [[function (where, batch)
    for i = 1, #batch do
        box.space[where]:insert(batch[i])
    end
end]]

local function start()
    box.schema.space.create("messages")
    box.space.messages:format({ { name = "value", type = "string", is_nullable = false }, { name = "time", type = "datetime", is_nullable = false } })
    box.space.messages:create_index("primary", { parts = { "value" } })
    box.schema.func.create('mass_insertion', {body = function_code})
end

return {
    start = start
}