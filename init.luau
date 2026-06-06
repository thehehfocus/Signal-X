--[[
MIT License

Copyright (c) 2026 OutOfFocus

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
--]]

--!optimize 2
--!strict
export type Signal<Params...> = {
	Id: any,

	Connect: (self: Signal<Params...>, Callback: (Params...) -> ()) -> Connection,
	Once: (self: Signal<Params...>, Callback: (Params...) -> ()) -> Connection,
	Wait: (self: Signal<Params...>) -> Params...,

	Fire: (self: Signal<Params...>, Params...) -> (),
	FireSync: (self: Signal<Params...>, Params...) -> (),
	FireDeferred: (self: Signal<Params...>, Params...) -> (),
	FireLate: (self: Signal<Params...>, Params...) -> (),
	FireCorout: (self: Signal<Params...>, Params...) -> (),
	FireSafe: (self: Signal<Params...>, ErrorHandler: (Error: string, Connection: Connection)->()?, Params...) -> (),

	DisconnectAll: (self: Signal<Params...>) -> (),
	Destroy: (self: Signal<Params...>) -> (),
}
type InternalSignal = {
	Head: InternalConnection?,
	Tail: InternalConnection?,
} & Signal<any>

export type Connection = {
	Signal: InternalSignal,	
	Disconnect: (Connection: Connection) -> (),
	Connected: boolean?
}
type InternalConnection = {
	-- [1] Prev: InternalConnection?,
	-- [2] Next: InternalConnection?,
	-- [3] Callback: (...any)->(),
} & Connection

-- Getting globals
const taskspawn = task.spawn
const taskdefer = task.defer
const coroutinecreate = coroutine.create
const coroutineresume = coroutine.resume
const coroutinerunning = coroutine.running
const coroutineyield = coroutine.yield
const coroutinestatus = coroutine.status
const tableinsert = table.insert

-- Pool
const Threads = {} :: {thread}
const function ReuseCall(Thread, Callback, ...)
	Callback(...)
	tableinsert(Threads, Thread)
end
const function ReuseThread()
	while true do
		ReuseCall(coroutineyield())
	end
end

const function Disconnect(Connection: InternalConnection)
	if not Connection.Connected then return end
	Connection.Connected = nil

	local Signal = Connection.Signal
	local Prev = Connection[1] :: InternalConnection?
	local Next = Connection[2] :: InternalConnection?

	if Prev then
		Prev[2] = Next
	else
		Signal.Tail = Next
	end

	if Next then
		Next[1] = Prev
	else
		Signal.Head = Prev
	end
end

const Storage = {} :: {[any]: Signal<any>}

const SignalClass = {}
SignalClass.__index = SignalClass
SignalClass.__tostring = function(Signal: InternalSignal)
	local __tostring = SignalClass.__tostring
	SignalClass.__tostring = nil :: any
	local address = tostring(Signal)
	SignalClass.__tostring = __tostring

	if Signal.Id then
		return `SignalX [{Signal.Id}] {address}`
	else
		return "SignalX, " .. address
	end
end

-- Uses task.spawn to fire the callbacks, immediate
function SignalClass.Fire(Signal: InternalSignal, ...)
	local Pointer = Signal.Tail
	while Pointer do
		local len = #Threads
		if len == 0 then
			local Thread = coroutinecreate(ReuseThread)
			coroutineresume(Thread)
			taskspawn(Thread, Thread, Pointer[3], ...)
		else
			local Thread = Threads[len]
			Threads[len] = nil
			taskspawn(Thread, Thread, Pointer[3], ...)
		end
		Pointer = Pointer[2]
	end
end

-- Uses task.defer to fire the callbacks, fires last in the frame
function SignalClass.FireDeferred(Signal: InternalSignal, ...)
	local Pointer = Signal.Tail
	while Pointer do
		local len = #Threads
		if len == 0 then
			local Thread = coroutinecreate(ReuseThread)
			coroutineresume(Thread)
			taskdefer(Thread, Thread, Pointer[3], ...)
		else
			local Thread = Threads[len]
			Threads[len] = nil
			taskdefer(Thread, Thread, Pointer[3], ...)
		end
		Pointer = Pointer[2]
	end
end

-- May be fastest fire function, less safe, uses coroutine.resume
function SignalClass.FireCorout(Signal: InternalSignal, ...)
	local Pointer = Signal.Tail
	while Pointer do
		local len = #Threads
		if len == 0 then
			local Thread = coroutinecreate(ReuseThread)
			coroutineresume(Thread)
			coroutineresume(Thread, Thread, Pointer[3], ...)
		else
			local Thread = Threads[len]
			Threads[len] = nil
			coroutineresume(Thread, Thread, Pointer[3], ...)
		end
		Pointer = Pointer[2]
	end
end

const function _errhandler(Error)
	warn(`Error calling callback: {Error or "unknown error"}`)
end
const function _safecall(Connection: InternalConnection, ErrorHandler, ...)
	xpcall(Connection[3], ErrorHandler and function(Error)
		ErrorHandler(Error, Connection)
	end or _errhandler, ...)
end
-- Wraps callbacks in an xpcall, slower
function SignalClass.FireSafe(Signal: InternalSignal, ErrorHandler: ()->()?, ...)
	local Pointer = Signal.Tail
	while Pointer do
		local len = #Threads
		if len == 0 then
			local Thread = coroutinecreate(ReuseThread)
			coroutineresume(Thread)
			taskspawn(Thread, Thread, _safecall, Pointer, ErrorHandler, ...)
		else
			local Thread = Threads[len]
			Threads[len] = nil
			taskspawn(Thread, Thread, _safecall, Pointer, ErrorHandler, ...)
		end
		Pointer = Pointer[2]
	end
end

-- Fires all callbacks in a single thread, simplest and fastest if none yield, a.k.a. uses task.wait()
function SignalClass.FireSync(Signal: InternalSignal, ...)
	local Pointer = Signal.Tail
	while Pointer do
		Pointer[3](...)

		Pointer = Pointer[2]
	end
end

const function _latefire(Signal: InternalSignal, ...)
	task.wait()
	Signal:Fire(...)
end
-- Fires all callbacks after a frame
function SignalClass.FireLate(Signal: InternalSignal, ...)
	local len = #Threads
	if len == 0 then
		local Thread = coroutinecreate(ReuseThread)
		coroutineresume(Thread)
		taskspawn(Thread, Thread, _latefire, Signal, ...)
	else
		local Thread = Threads[len]
		Threads[len] = nil
		taskspawn(Thread, Thread, _latefire, Signal, ...)
	end
end

-- Avoiding table indexing and allowing inlining
const function Connect(Signal: InternalSignal, Callback) : InternalConnection
	local Head = Signal.Head

	local Connection = {
		[1] = Head,
		[3] = Callback,

		Connected = true,
		Signal = Signal,
		Disconnect = Disconnect,
	} :: InternalConnection

	if Head then
		Head[2] = Connection
	else
		Signal.Tail = Connection
	end
	Signal.Head = Connection

	return Connection
end
SignalClass.Connect = Connect 

function SignalClass.Once(Signal: InternalSignal, Callback)
	local Connection : InternalConnection
	Connection = Connect(Signal, function()
		Disconnect(Connection)
		Callback()
	end)

	return Connection
end

function SignalClass.Wait(Signal: InternalSignal)
	local Thread = coroutinerunning()

	local Connection: InternalConnection
	Connection = Connect(Signal, function(...)
		Disconnect(Connection)
		if coroutinestatus(Thread) == "suspended" then
			taskspawn(Thread, ...)
		end
	end)

	return coroutineyield()
end

function SignalClass.DisconnectAll(Signal: InternalSignal)
	local Pointer = Signal.Tail
	while Pointer do
		Pointer.Connected = nil
		Pointer = Pointer[2]
	end
	Signal.Tail = nil
	Signal.Head = nil
end

function SignalClass.Destroy(Signal: InternalSignal)
	Signal:DisconnectAll()
	local Id = Signal.Id
	if Id then
		Storage[Id] = nil
		Signal.Id = nil
	end
end

const function CreateSignal(Id: any)
	local Signal = setmetatable({}, SignalClass) :: Signal<any>
	if Id then
		Storage[Id] = Signal
		Signal.Id = Id
	end
	return Signal
end

const function GetSignal(Id: any) : Signal<any>
	return Storage[Id]
end

return setmetatable({
	new = CreateSignal,
	get = GetSignal,
}, {
	__call = CreateSignal
})