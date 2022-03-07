#if INTERACTIVE
#r "nuget: Npgsql"
#load "../Contracts/Contracts.fs"
#load "ThinLayer.fs"
#load "../Server/Engine.fs"
#endif

open Npgsql
open System.Threading
open EventStore
let tokenSource = new CancellationTokenSource() 
let connStr = "Host=localhost;Port=5432;Database=message_store;Username=postgres;Password=summersun"
let connection = new NpgsqlConnection(connStr)
let rres = Sql.setRole connection tokenSource.Token "message_store" |> Async.RunSynchronously
let func = [ Sql.createParameter Sql.Varchar "stream_name" "someStream-123"] |> Sql.createFunc connection "get_stream_messages"
let result = Sql.executeQueryAsync connection func tokenSource.Token (fun r -> System.Guid.Parse(r.string "id"), r.string "type", r.int64 "position", r.stringOrNone "data", r.stringOrNone "metadata", r.dateTime "time") |> Async.RunSynchronously

func.Dispose()
connection.Close()
connection.Dispose()

let guid = System.Guid.NewGuid()
guid.ToString()
guid.ToString("N")

let engine = Engine.MessageStore(connStr)

let message = {
    Native.Contracts.UnrecordedMessage.Id = System.Guid.NewGuid()
    Native.Contracts.UnrecordedMessage.EventType = "Created"
    Native.Contracts.UnrecordedMessage.Metadata = Some "{\"message-type\":\"event\", \"message-version\":1, \"system\":\"delfin\"}"
    Native.Contracts.UnrecordedMessage.Data = "{\"username\":\"sebfia\", \"mapped-logon-name\":\"Sebasterich\", \"logon-system\":\"ADC\"}"
}

let expectedVersion = -1L

let version = engine.WriteStreamMessage("user-123", message, expectedVersion, tokenSource.Token) |> Async.RunSynchronously

let messages = engine.GetLastStreamMessage("user-123") |> Async.RunSynchronously