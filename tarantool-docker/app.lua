#!/usr/bin/env tarantool

box.cfg {
    listen = 3301;
}

local m = require('configuration').start()