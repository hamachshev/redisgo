package main

import (
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"GET":     get,
	"SET":     set,
	"HGET":    hget,
	"HSET":    hset,
	"HGETALL": hgetall,
}

func ping(args []Value) Value {
	if len(args) == 1 {
		return Value{typ: "string", str: args[0].bulk}
	}
	return Value{typ: "string", str: "PONG"}

}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "Wrong number of args for 'SET' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value

	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "Wrong number of args for 'GET' command"}
	}

	key := args[0].bulk

	SETsMu.Lock()
	value, ok := SETs[key]

	SETsMu.Unlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) < 3 {
		return Value{typ: "error", str: "Wrong number of args for 'HSET' command"}
	}

	if len(args[1:])%2 != 0 {
		return Value{typ: "error", str: "Wrong number of args for 'HSET' command -- keys and values must be equal"}
	}

	hset := args[0].bulk
	HSETsMu.Lock()
	if _, ok := HSETs[hset]; !ok {
		HSETs[hset] = map[string]string{}
	}
	for i := 1; i < len(args); i += 2 {
		HSETs[hset][args[i].bulk] = args[i+1].bulk
	}
	HSETsMu.Unlock()
	return Value{typ: "string", str: "OK"}
}
func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "Wrong number of args for 'HSET' command"}
	}

	hset := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hset][key]

	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}

}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "Wrong number of args for 'HGETALL' command"}
	}

	hset := args[0].bulk

	HSETsMu.Lock()

	values, ok := HSETs[hset]
	HSETsMu.Unlock()

	if !ok {
		return Value{typ: "null"}
	}

	var array []Value

	for key, value := range values {
		array = append(array, Value{typ: "bulk", bulk: key})
		array = append(array, Value{typ: "bulk", bulk: value})
	}

	return Value{typ: "array", array: array}
}
