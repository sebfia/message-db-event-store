namespace EventStore

module Engine =
    open System
    open System.Threading
    open EventStore.Native
    open Npgsql

    type MessageStore(connectionString) =
        do
            if String.IsNullOrEmpty(connectionString) then failwith "Invalid connection string!"
        
        let readMessage (reader: Sql.RowReader) = 
            {
                Id = Guid.Parse(reader.string "id")
                StreamName = reader.string "stream_name"
                CreatedTimeUTC = reader.dateTime "time"
                Version = reader.int64 "position"
                EventType = reader.string "type"
                Metadata = reader.stringOrNone "metadata"
                Data = reader.string "data"
            }
        let createWriteFunc connection streamName (eventId: Guid) eventType data metadata expectedVersion = 
            [
                streamName |> Sql.stringParameter "stream_name" |> Some
                (eventId.ToString()) |> Sql.stringParameter "id" |> Some
                eventType |> Sql.stringParameter "type" |> Some
                data |> Sql.jsonParameter "data" |> Some
                Option.map (fun v -> Sql.jsonParameter "metadata" v) metadata
                Option.map (fun v -> Sql.int64Parameter "expected_version" v) expectedVersion
            ]
            |> List.choose id
            |> Sql.createFunc connection "write_message"
        member __.GetStreamMessages (streamName:string, ?position:int64, ?batchSize:int64, ?cancellationToken:CancellationToken) = async {
            let token = defaultArg cancellationToken CancellationToken.None
            use connection = new NpgsqlConnection(connectionString)
            do! Sql.setRole connection token "message_store" |> Async.Ignore
            let func =
                [ 
                    Sql.stringParameter "stream_name" streamName |> Some
                    Option.map (fun v -> Sql.int64Parameter "position" v) position
                    Option.map (fun v -> Sql.int64Parameter "batch_size" v) batchSize
                ]
                |> List.choose id 
                |> Sql.createFunc connection "get_stream_messages"
            return! Sql.executeQueryAsync connection func token readMessage
        }

        member __.WriteStreamMessage (streamName: string, message: UnrecordedMessage, ?expectedVersion: int64, ?cancellationToken: CancellationToken) = async {
            let token = defaultArg cancellationToken CancellationToken.None
            use connection = new NpgsqlConnection(connectionString)
            do! Sql.setRole connection token "message_store" |> Async.Ignore
            let func = createWriteFunc connection streamName message.Id message.Data message.Data message.Metadata expectedVersion
            match! Sql.executeScalarAsync<int64> connection [|func|] token with
            | Error exn -> return Error exn
            | Ok lst -> return lst |> List.head |> Ok
        }
        
        member __.WriteStreamMessages (streamName: string, messages: UnrecordedMessage array, ?expectedVersion: int64, ?cancellationToken: CancellationToken) = async {
            let token = defaultArg cancellationToken CancellationToken.None
            use connection = new NpgsqlConnection(connectionString)
            do! Sql.setRole connection token "message_store" |> Async.Ignore
            let createWriteFunc' = createWriteFunc connection streamName
            let funcs = messages |> Array.mapi (fun i m -> createWriteFunc' m.Id m.EventType m.Data m.Metadata (if i > 0 then None else expectedVersion))
            return! Sql.executeScalarAsync connection funcs token
        }

        member __.GetLastStreamMessage (streamName: string, ?cancellationToken) = async {
            let token = defaultArg cancellationToken CancellationToken.None
            use connection = new NpgsqlConnection(connectionString)
            do! Sql.setRole connection token "message_store" |> Async.Ignore
            let func =
                [ 
                    Sql.stringParameter "stream_name" streamName 
                ]
                |> Sql.createFunc connection "get_last_stream_message"
            match! Sql.executeQueryAsync connection func token readMessage with
            | Ok lst -> return Ok (lst |> List.tryHead)
            | Error exn -> return Error exn
        }

        member __.GetLastStreamVersion (streamName: string, ?cancellationToken) = async {
            let token = defaultArg cancellationToken CancellationToken.None
            use connection = new NpgsqlConnection(connectionString)
            do! Sql.setRole connection token "message_store" |> Async.Ignore
            let func =
                [ 
                    Sql.stringParameter "stream_name" streamName 
                ]
                |> Sql.createFunc connection "stream_version"
            match! Sql.executeQueryAsync connection func token (fun r -> r.int64OrNone "") with
            | Ok lst -> return Ok (lst |> List.head |> function | Some i -> i | _ -> -1L)
            | Error exn -> return Error exn
        }