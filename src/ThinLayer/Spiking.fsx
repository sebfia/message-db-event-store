#if INTERACTIVE
// #r "nuget: Npgsql"
#r "nuget: FsPickler"
#r "../Client/bin/Debug/net6.0/Contracts.dll"
#r "../Client/bin/Debug/net6.0/Client.dll"
// #load "../Contracts/Contracts.fs"
// #load "ThinLayer.fs"
// #load "../Server/Engine.fs"
#endif

// open Npgsql
// open System.Threading
// open EventStore
// let tokenSource = new CancellationTokenSource() 
// let connStr = "Host=localhost;Port=5432;Database=message_store;Username=postgres;Password=summersun"
// let connection = new NpgsqlConnection(connStr)
// let rres = Sql.setRole connection tokenSource.Token "message_store" |> Async.RunSynchronously
// let func = [ Sql.createParameter Sql.Varchar "stream_name" "someStream-123"] |> Sql.createFunc connection "get_stream_messages"
// let result = Sql.executeQueryAsync connection func tokenSource.Token (fun r -> System.Guid.Parse(r.string "id"), r.string "type", r.int64 "position", r.stringOrNone "data", r.stringOrNone "metadata", r.dateTime "time") |> Async.RunSynchronously

// func.Dispose()
// connection.Close()
// connection.Dispose()

// let guid = System.Guid.NewGuid()
// guid.ToString()
// guid.ToString("N")

// let engine = Engine.MessageStore(connStr)

// let message = {
//     Native.Contracts.UnrecordedMessage.Id = System.Guid.NewGuid()
//     Native.Contracts.UnrecordedMessage.EventType = "Created"
//     Native.Contracts.UnrecordedMessage.Metadata = Some "{\"message-type\":\"event\", \"message-version\":1, \"system\":\"delfin\"}"
//     Native.Contracts.UnrecordedMessage.Data = "{\"username\":\"sebfia\", \"mapped-logon-name\":\"Sebasterich\", \"logon-system\":\"ADC\"}"
// }

// let expectedVersion = 0L

// let version = engine.WriteStreamMessage("user-123", message, expectedVersion, tokenSource.Token) |> Async.RunSynchronously

// let messages = engine.GetLastStreamMessage("user-123") |> Async.RunSynchronously

open System
open System.Net
open System.Net.Sockets
// open MBrace.FsPickler
open EventStore.Native

// let binarySerializer = FsPickler.CreateBinarySerializer()

// let request = AppendMessage ("user_123",1L,{Id=System.Guid.NewGuid();EventType="Test";Metadata=None;Data="{\"Bloshuettn\":\"Bist Deppat\"}"})

let tcp = new TcpClient()
tcp.Connected
tcp.Connect(IPEndPoint(IPAddress.Loopback, 9009))
let stream = tcp.GetStream()
let msg = "HHHHHHHHEEEEEELLLLLLOOOOOOOO\n" |> Text.Encoding.UTF8.GetBytes 
stream.Write(msg, 0, msg.Length)

tcp.Dispose()
stream.Dispose()

let client = EventStore.Client.createClient IPAddress.Loopback 9781

let response = client.AppendMessage "user-123" 2L {Id=Guid.NewGuid();EventType="Test";Metadata=Some "{'system': 'delfin','message-type': 'event','message-version': 1}";Data="{\"Bloshuettn\":\"Bist Deppat\"}"} |> Async.RunSynchronously
let err = response |> function | Error e -> e | _ -> failwith "unexpected"
printfn "%A" err

open System
open System.Text.RegularExpressions

let regex = Regex("Wrong expected version: (?<expected_version>\d+) \(Stream: (?<stream>\w+[\-\/\+\@\#]?\w+), Stream Version: (?<stream_version>\-?\d+)\)", RegexOptions.Compiled ||| RegexOptions.Singleline ||| RegexOptions.CultureInvariant)
regex.IsMatch "Wrong expected version: 1 (Stream: user_123, Stream Version: -1)"
regex.IsMatch "Wrong expected version: 1 (Stream: user-123, Stream Version: 2)"
let matches = regex.Matches "Wrong expected version: 1 (Stream: user_123, Stream Version: -1)"
let m = regex.Match "Wrong expected version: 1 (Stream: user_123, Stream Version: -1)"
let m = regex.Match "Ich was nix"
m.Success
m.Groups["expected_version"].Value
m.Groups["stream"].Value
m.Groups["stream_version"].Value

#r "nuget: FSharp.SystemTextJson"
open System.Text.Json
open System.Text.Json.Serialization
open System.Text.RegularExpressions

let getEventType =
    let tagRegex = Regex("\\{\\\"(?<EventType>\\w+)\\\"\\:(.*)", RegexOptions.Compiled ||| RegexOptions.Singleline ||| RegexOptions.CultureInvariant)
    fun (s: string) ->
        match tagRegex.Match(s) with
        | m when m.Success -> Some m.Groups["EventType"].Value
        | _ -> None


let opts = JsonSerializerOptions(JsonSerializerDefaults())
opts.Converters.Add(JsonFSharpConverter(unionEncoding=(JsonUnionEncoding.ExternalTag|||JsonUnionEncoding.UnwrapOption|||JsonUnionEncoding.UnwrapRecordCases)))
let serialize<'a> (a: 'a) = 
    System.Text.Json.JsonSerializer.Serialize(a, opts)

let deserialize<'a> (s: string) =
    System.Text.Json.JsonSerializer.Deserialize<'a>(s, opts)

type Event =
    | UserAdded of loginName:string*name:string*shorthand:string
    | SiteAdded of name:string*symbol:string*country:string
    | CategoryAdded of name:string*threshold:int

type Test = {
    [<JsonPropertyName("system")>]
    LoginSystem: string
    [<JsonPropertyName("message-type")>]
    MessageType: string
    [<JsonPropertyName("message-version")>]
    MessageVersion: int
}

let metadata = {
    LoginSystem = "delfin"
    MessageType = "event"
    MessageVersion = 1
}

let metaStr = serialize metadata

let testJson = serialize (UserAdded("sebfia","Sebastian Fialka","SF"))

let tagName = testJson |> getEventType

let des = deserialize<Event> testJson

printfn "%s" testJson

let client = EventStore.Client.createClient IPAddress.Loopback 9781

let response = client.AppendMessage "user-123" 3L {Id=Guid.NewGuid(); EventType="Test"; Metadata=Some metaStr; Data="{\"Bloshuettn\":\"Bist Deppat\"}"} |> Async.RunSynchronously
let messages = client.ReadStreamMessagesForward "user-123" None BatchSize.All |> Async.RunSynchronously

let version = client.ReadMessageStoreVersion() |> Async.RunSynchronously