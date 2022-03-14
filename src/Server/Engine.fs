namespace EventStore

module Engine =
    open System
    open System.Threading
    open System.Text.RegularExpressions
    open EventStore.Native
    open Npgsql
    open NATS.Client.Internals

    let onSecondChoice f = function
        | Choice2Of2 x -> f x |> Some
        | _ -> None
    let unwrapAggregateExn (ex: AggregateException) =
        ex.InnerExceptions |> Seq.head
    let handleAggregateException (ex: exn) =
        match ex with 
        | :? AggregateException as e -> e
        | e -> raise e


    let (|ExpectedVersionError|_|) =
        let regex = Regex("Wrong expected version: (?<expected_version>\d+) \(Stream: (?<stream>\w+[\-]?\w+), Stream Version: (?<stream_version>\-?\d+)\)", RegexOptions.Compiled ||| RegexOptions.Singleline ||| RegexOptions.CultureInvariant)
        fun (str: string) ->
            match regex.Match str with
            | m when m.Success ->
                Some (WrongExpectedVersion (m.Groups["expected_version"].Value |> int64, m.Groups["stream"].Value, m.Groups["stream_version"].Value |> int64))
            | _ -> None
    let (|MustBeAStreamName|_|) =
        let regex = Regex("Must be a stream name: (?<wrong_stream_name>.*)", RegexOptions.Compiled ||| RegexOptions.Singleline ||| RegexOptions.CultureInvariant)
        fun (str: string) ->
            match regex.Match str with
            | m when m.Success ->
                Some (StreamNameIncorrect m.Groups["wrong_stream_name"].Value)
            | _ -> None
    let (|FailedToConnect|_|) =
        let regex = Regex("Failed to connect to (?<host>.*)", RegexOptions.Compiled ||| RegexOptions.Singleline ||| RegexOptions.CultureInvariant)
        fun (str: string) ->
            match regex.Match str with
            | m when m.Success ->
                Some (UnableToConnectToDb m.Groups["host"].Value)
            | _ -> None

    type MessageStore(connectionString) =
        let logger = NLog.LogManager.GetLogger("Db-Engine")
        do
            logger.Trace "Starting up message-store engine."
            if String.IsNullOrEmpty(connectionString) then failwith "Invalid connection string!"

        let toDbError (exn: exn) = 
            match exn with
            | :? PostgresException as e ->
                match e.MessageText with
                | ExpectedVersionError e -> e
                | MustBeAStreamName e -> e
                | _ -> UnexpectedException (sprintf "%A" e)
            | :? NpgsqlException as e ->
                match e.Message with
                | FailedToConnect e -> e
                | _ -> UnexpectedException (sprintf "%A" e)
            | e -> UnexpectedException (sprintf "%A" e)

        
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
                (eventId.ToString()) |> Sql.stringParameter "id" |> Some
                streamName |> Sql.stringParameter "stream_name" |> Some
                eventType |> Sql.stringParameter "type" |> Some
                data |> Sql.jsonParameter "data" |> Some
                metadata |> Option.map (Sql.jsonParameter "metadata") 
                expectedVersion |> Option.map (Sql.int64Parameter "expected_version")
            ]
            |> List.choose id
            |> Sql.createFunc connection "write_message"

        member __.GetStreamMessages (streamName:string, ?position:int64, ?batchSize:int64, ?cancellationToken:CancellationToken) = async {
            let extToken = defaultArg cancellationToken CancellationToken.None
            use timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(30.))
            use cts = CancellationTokenSource.CreateLinkedTokenSource(extToken, timeoutCts.Token)
            use connection = new NpgsqlConnection(connectionString)
            let! rs = Sql.setRole connection cts.Token "message_store" |> Async.Catch
            match rs |> onSecondChoice (handleAggregateException >> unwrapAggregateExn >> toDbError) with
            | Some err -> 
                if logger.IsErrorEnabled then sprintf "Error setting role: %A" err |> logger.Error
                return Error err
            | _ ->
                let func =
                    [ 
                        Sql.stringParameter "stream_name" streamName |> Some
                        Option.map (fun v -> Sql.int64Parameter "position" v) position
                        Option.map (fun v -> Sql.int64Parameter "batch_size" v) batchSize
                    ]
                    |> List.choose id 
                    |> Sql.createFunc connection "get_stream_messages"
                match! Sql.executeQueryAsync connection func cts.Token readMessage with
                | Error exn -> return Error (exn |> toDbError)
                | Ok lst -> return lst |> Ok
        }

        member __.WriteStreamMessage (streamName: string, message: UnrecordedMessage, ?expectedVersion: int64, ?cancellationToken: CancellationToken) = async {
            let extToken = defaultArg cancellationToken CancellationToken.None
            use timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5.))
            use cts = CancellationTokenSource.CreateLinkedTokenSource(extToken, timeoutCts.Token)
            use connection = new NpgsqlConnection(connectionString)
            let! rs = Sql.setRole connection cts.Token "message_store" |> Async.Catch
            match rs |> onSecondChoice (handleAggregateException >> unwrapAggregateExn >> toDbError) with
            | Some err -> 
                if logger.IsErrorEnabled then sprintf "Error setting role: %A" err |> logger.Error
                return Error err
            | _ ->
                let func = createWriteFunc connection streamName message.Id message.EventType message.Data message.Metadata expectedVersion
                match! Sql.executeScalarAsync<int64> connection [|func|] cts.Token with
                | Error exn -> return Error (exn |> toDbError)
                | Ok lst -> return lst |> List.head |> Ok
        }
        
        member __.WriteStreamMessages (streamName: string, messages: UnrecordedMessage array, ?expectedVersion: int64, ?cancellationToken: CancellationToken) = async {
            let extToken = defaultArg cancellationToken CancellationToken.None
            use timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(15.))
            use cts = CancellationTokenSource.CreateLinkedTokenSource(extToken, timeoutCts.Token)
            use connection = new NpgsqlConnection(connectionString)
            let! rs = Sql.setRole connection cts.Token "message_store" |> Async.Catch
            match rs |> onSecondChoice (handleAggregateException >> unwrapAggregateExn >> toDbError) with
            | Some err -> 
                if logger.IsErrorEnabled then sprintf "Error setting role: %A" err |> logger.Error
                return Error err
            | _ ->
                let createWriteFunc' = createWriteFunc connection streamName
                let funcs = messages |> Array.mapi (fun i m -> createWriteFunc' m.Id m.EventType m.Data m.Metadata (if i > 0 then None else expectedVersion))
                match! Sql.executeScalarAsync<int64> connection funcs cts.Token with
                | Error exn -> return Error (exn |> toDbError)
                | Ok lst -> return lst |> Ok
        }

        member __.GetLastStreamMessage (streamName: string, ?cancellationToken) = async {
            let extToken = defaultArg cancellationToken CancellationToken.None
            use timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5.))
            use cts = CancellationTokenSource.CreateLinkedTokenSource(extToken, timeoutCts.Token)
            use connection = new NpgsqlConnection(connectionString)
            let! rs = Sql.setRole connection cts.Token "message_store" |> Async.Catch
            match rs |> onSecondChoice (handleAggregateException >> unwrapAggregateExn >> toDbError) with
            | Some err -> 
                if logger.IsErrorEnabled then sprintf "Error setting role: %A" err |> logger.Error
                return Error err
            | _ ->
                let func =
                    [ 
                        Sql.stringParameter "stream_name" streamName 
                    ]
                    |> Sql.createFunc connection "get_last_stream_message"
                match! Sql.executeQueryAsync connection func cts.Token readMessage with
                | Ok lst -> return Ok (lst |> List.tryHead)
                | Error exn -> return Error (exn |> toDbError)
        }

        member __.GetStreamVersion (streamName: string, ?cancellationToken) = async {
            let extToken = defaultArg cancellationToken CancellationToken.None
            use timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5.))
            use linkedCts = CancellationTokenSource.CreateLinkedTokenSource(extToken, timeoutCts.Token)
            use connection = new NpgsqlConnection(connectionString)
            let! rs = Sql.setRole connection linkedCts.Token "message_store" |> Async.Catch
            match rs |> onSecondChoice (handleAggregateException >> unwrapAggregateExn >> toDbError) with
            | Some err -> 
                if logger.IsErrorEnabled then sprintf "Error setting role: %A" err |> logger.Error
                return Error err
            | _ ->
                let func =
                    [ 
                        Sql.stringParameter "stream_name" streamName 
                    ]
                    |> Sql.createFunc connection "stream_version"
                match! Sql.executeQueryAsync connection func linkedCts.Token (fun r -> r.int64OrNone "") with
                | Ok lst -> return Ok (lst |> List.head |> function | Some i -> i | _ -> -1L)
                | Error exn -> return Error (exn |> toDbError)
        }

        member __.GetMessageStoreVersion (?cancellationToken) = async {
            let extToken = defaultArg cancellationToken CancellationToken.None
            use timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5.))
            use linkedCts = CancellationTokenSource.CreateLinkedTokenSource(extToken, timeoutCts.Token)
            use connection = new NpgsqlConnection(connectionString)
            let! rs = Sql.setRole connection linkedCts.Token "message_store" |> Async.Catch
            match rs |> onSecondChoice (handleAggregateException >> unwrapAggregateExn >> toDbError) with
            | Some err -> 
                if logger.IsErrorEnabled then sprintf "Error setting role: %A" err |> logger.Error
                return Error err
            | _ ->
                let func = [ ] |> Sql.createFunc connection "message_store_version"
                match! Sql.executeQueryAsync connection func linkedCts.Token (fun r -> r.stringOrNone "message_store_version") with
                | Ok lst -> return Ok (lst |> List.head |> function | Some s -> s | _ -> "-1")
                | Error exn -> return Error (exn |> toDbError)
        }