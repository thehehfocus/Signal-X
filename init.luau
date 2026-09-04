--[[
MIT License
By OutOfFocus (thehehfocus on github)
--]]

--!optimize 2
--!strict
export type Signal<Params... = ...any> = {
	--[[ Properties ]]--
	Id: any,

	--[[ Connect operations ]]--
	Connect: (self: Signal<Params...>, Callback: (Params...) -> ()) -> InternalConnection,
	Disconnect: (Connection: InternalConnection) -> (),
	Once: (self: Signal<Params...>, Callback: (Params...) -> ()) -> InternalConnection,
	Wait: (self: Signal<Params...>, Filter: ((Params...) -> boolean?)?) -> Params...,

	--[[ Fire operations ]]--
	Fire: (self: Signal<Params...>, Params...) -> (),
	FireSync: (self: Signal<Params...>, Params...) -> (),
	FireDeferred: (self: Signal<Params...>, Params...) -> (),
	FireProtected: (self: Signal<Params...>, Sync: boolean?, ErrorHandler: (Error: string)->(), Params...) -> (),

	--[[ Utilities ]]--
	DisconnectAll: (self: Signal<Params...>) -> (),
	Destroy: (self: Signal<Params...>) -> (),
}
type InternalSignal = {
	Head: InternalConnection?,
	Tail: InternalConnection?,
} & Signal

export type ConnectionResult = (Disconnect: nil) -> ()
type InternalConnection = {
	-- [1] Prev: InternalConnection?,
	-- [2] Next: InternalConnection?,
	-- [3] Callback: (...any)->(),
	-- [4] Connected: boolean?,
	[number]: any
}

-- Pool
const Threads = {} :: {thread}
const ThreadLimit = 75

const function ReuseCall(Thread, Callback, ...)
	Callback(...)
	
	local len = #Threads
	if len < ThreadLimit then
		Threads[len + 1] = Thread
	end
end
const function ReuseThread()
	while true do
		ReuseCall(coroutine.yield())
	end
end

const function Disconnect(Signal: any, Connection: InternalConnection)
	if not Connection[4] then return end
	Connection[4] = nil

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

const Storage = {} :: { [any]: Signal }

const SignalClass = {}
SignalClass.__index = SignalClass
SignalClass.__tostring = function(Signal: InternalSignal)
	local __tostring = SignalClass.__tostring
	SignalClass.__tostring = nil :: any
	local address = tostring(Signal)
	SignalClass.__tostring = __tostring

	if Signal.Id then
		return `(SignalX, id: {Signal.Id}, {address})`
	else
		return "(SignalX, " .. address .. ")"
	end
end

-- Uses task.spawn to fire the callbacks, immediate
function SignalClass.Fire(Signal: InternalSignal, ...)
	local Pointer = Signal.Tail
	while Pointer do
		local len = #Threads
		if len == 0 then
			local Thread = coroutine.create(ReuseThread)
			coroutine.resume(Thread)
			task.spawn(Thread, Thread, Pointer[3], ...)
		else
			local Thread = Threads[len]
			Threads[len] = nil
			task.spawn(Thread, Thread, Pointer[3], ...)
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
			local Thread = coroutine.create(ReuseThread)
			coroutine.resume(Thread)
			task.defer(Thread, Thread, Pointer[3], ...)
		else
			local Thread = Threads[len]
			Threads[len] = nil
			task.defer(Thread, Thread, Pointer[3], ...)
		end
		Pointer = Pointer[2]
	end
end

const function errHandler(Error)
	warn(`Error calling callback: {Error} \n {debug.traceback()}`)
end
-- Wraps callbacks in an xpcall
function SignalClass.FireProtected(Signal: InternalSignal, Sync: boolean?, ErrorHandler, ...)
	local Pointer = Signal.Tail

	while Pointer do
		local len = #Threads
		if len == 0 then
			local Thread = coroutine.create(ReuseThread)
			coroutine.resume(Thread)
			task.spawn(Thread, Thread, xpcall, Pointer[3], ErrorHandler or errHandler, ...)
		else
			local Thread = Threads[len]
			Threads[len] = nil
			task.spawn(Thread, Thread, xpcall, Pointer[3], ErrorHandler or errHandler, ...)
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

const function Connect(Signal: InternalSignal, Callback) : InternalConnection
	local Head = Signal.Head

	local Connection = {
		[1] = Head,
		[2] = nil,
		[3] = Callback,
		[4] = true,
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
SignalClass.Disconnect = Disconnect

function SignalClass.Once(Signal: InternalSignal, Callback)
	local Connection : InternalConnection
	Connection = Connect(Signal, function()
		Disconnect(Signal, Connection)
		Callback()
	end)

	return Connection
end

function SignalClass.Wait(Signal: InternalSignal, Filter: ((...any) -> boolean?)?)
	local Thread = coroutine.running()

	local Connection: InternalConnection
	Connection = Connect(Signal, function(...)
		if (Filter and not Filter(...)) then
			return
		end

		Disconnect(Signal, Connection)
		if coroutine.status(Thread) == "suspended" then
			task.spawn(Thread, ...)
		end
	end)

	return coroutine.yield()
end

function SignalClass.DisconnectAll(Signal: InternalSignal)
	local Pointer = Signal.Tail
	while Pointer do
		Pointer[4] = nil
		Pointer = Pointer[2]
	end
	Signal.Tail = nil
	Signal.Head = nil
end

function SignalClass.Destroy(Signal: InternalSignal)
	Signal:DisconnectAll()
	local Id = Signal.Id
	local InStorage = Id and Storage[Id]
	if InStorage == Signal then
		Storage[Id] = nil
	end
end

const function CreateSignal(Id: any) : Signal
	local Signal = setmetatable({}, SignalClass)
	if Id then
		Storage[Id] = Signal :: any
		Signal.Id = Id
	end
	return Signal :: any
end

const function GetSignal(Id: any) : Signal
	return Storage[Id]
end

local Module = {}
Module.new = CreateSignal
Module.get = GetSignal

Module.Wrap = function<T...>(rbxSignal: RBXScriptSignal<T...>) : (Signal<T...>, ConnectionResult)
	local Signal = CreateSignal()
	return Signal::any, rbxSignal:Connect(function(...)
		Signal:Fire(...)
	end)
end

Module.SignalMT = SignalClass

return Module
