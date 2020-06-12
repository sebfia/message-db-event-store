#if INTERACTIVE
#I "../../packages/spiking/Microsoft.Bcl.AsyncInterfaces/lib/netstandard2.0"
#r "netstandard"
#r "System.Threading"
#r "../../packages/spiking/Npgsql/lib/netstandard2.0/Npgsql.dll"
#load "ThinLayer.fs"
#endif

open Npgsql
open System.Threading
open EventStore
let tokenSource = new CancellationTokenSource() 
let connStr = "Host=localhost;Port=5432;Database=message_store;Username=postgres;Password=summersun"
let connection = new NpgsqlConnection(connStr)
let result = Sql.setRole connection tokenSource.Token "message_store" |> Async.RunSynchronously
let func = [ Sql.createParameter Sql.Varchar "stream_name" "someStream-123"] |> Sql.createFunc connection "get_stream_messages"
let result = Sql.executeQueryAsync connection func tokenSource.Token (fun r -> System.Guid.Parse(r.string "id"), r.string "type", r.int64 "position", r.stringOrNone "data", r.stringOrNone "metadata", r.dateTime "time") |> Async.RunSynchronously

func.Dispose()
connection.Close()
connection.Dispose()

let guid = System.Guid.NewGuid()
guid.ToString()
guid.ToString("N")