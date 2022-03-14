namespace EventStore.Native

module TcpServer =
    open System
    open System.Net
    open System.Net.Sockets
    open System.Threading
    open MBrace.FsPickler
    open EventStore.Native.Contracts
    open EventStore

    type DateTime with
        static member private epoch = DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)

        member x.ToEpoch () = (x - DateTime.epoch).TotalSeconds |> Convert.ToInt64

    
    let logger = NLog.LogManager.GetLogger("Tcp-Server")
    let start (connectionString: string) (endpoint: IPEndPoint) =
        if logger.IsDebugEnabled then sprintf "Starting native interface with connection string: %s" connectionString |> logger.Debug
        let connectionTimeout = TimeSpan.FromMinutes(5.)
        let serializer = FsPickler.CreateBinarySerializer()
        let server = TcpListener(endpoint)
        let messageStore = Engine.MessageStore(connectionString)
        
        let appendEvent (client: TcpClient) (streamName,expectedVersion,(unrecordedMessage: UnrecordedMessage)) = async {
            try
                let! result = messageStore.WriteStreamMessage(streamName, unrecordedMessage, expectedVersion)
                let response =
                    match result  with
                    | Error err -> 
                        if logger.IsWarnEnabled then sprintf "Error appending event to stream: %s%s%A" streamName Environment.NewLine err |> logger.Warn
                        Error err
                    | Ok recordedMessage -> Ok (MessageAppended recordedMessage)
                let bytes = serializer.Pickle response
                let stream = client.GetStream()
                do! stream.AsyncWriteBytes bytes
            with 
            // :? Npgsql.PostgresException as e -> 
            //     if logger.IsErrorEnabled then sprintf "Error responding to tcp-client: %A" e |> logger.Error
            | e -> logger.Error e
        }
        let appendEventsToStream (client: TcpClient) (streamName,expectedVersion,(unrecordedMessages: UnrecordedMessage array)) = async {
            try
                let! result = messageStore.WriteStreamMessages(streamName, unrecordedMessages, expectedVersion)
                let response =
                    match result with
                    | Ok nums -> Ok (MessagesAppended(streamName,nums |> List.max))
                    | Error err ->
                        if logger.IsWarnEnabled then sprintf "Error appending events to stream: %s%s%A" streamName Environment.NewLine err |> logger.Warn
                        Error err
                let bytes = serializer.Pickle response
                let stream = client.GetStream()
                do! stream.AsyncWriteBytes bytes
            with e -> if logger.IsErrorEnabled then sprintf "Error responding to tcp-client: %A" e |> logger.Error
        }
        let readStreamEvents (client: TcpClient) (streamName,fromMessageNumber,numEvents) = async {
            try
                if logger.IsTraceEnabled then sprintf "Reading stream events for stream %s" streamName |> logger.Trace
                let batchSize = function | All -> -1L | Limited num -> num
                let position = defaultArg fromMessageNumber 0L
                let! result = messageStore.GetStreamMessages(streamName,position,numEvents |> batchSize)
                if logger.IsTraceEnabled then sprintf "Got result from engine: %A" result |> logger.Trace
                let response = 
                    match result with
                    | Ok messages -> 
                        if logger.IsTraceEnabled then sprintf "Creating Ok response for %i recorded events." messages.Length |> logger.Trace
                        Ok (StreamMessagesRead(streamName,messages |> List.toArray))
                    | Error err ->
                        if logger.IsWarnEnabled then sprintf "Error reading events from stream: %s%s%A" streamName Environment.NewLine err |> logger.Warn
                        Error err
                if logger.IsTraceEnabled then sprintf "Created response. Serializing..." |> logger.Trace
                let bytes = serializer.Pickle response
                if logger.IsTraceEnabled then sprintf "Writing %i bytes to stream..." bytes.Length |> logger.Trace
                let stream = client.GetStream()
                do! stream.AsyncWriteBytes bytes
            with e -> if logger.IsErrorEnabled then sprintf "Error responding to tcp-client: %A" e |> logger.Error
        }
        let readMessageStoreVersion (client: TcpClient) = async {
            try
                if logger.IsTraceEnabled then "Reading message store version." |> logger.Trace
                let! result = messageStore.GetMessageStoreVersion ()
                if logger.IsTraceEnabled then sprintf "Got result from engine: %A" result |> logger.Trace
                let response = 
                    match result with
                    | Ok version -> 
                        if logger.IsTraceEnabled then "Creating Ok response for message store version." |> logger.Trace
                        Ok (MessageStoreVersionRead version)
                    | Error err ->
                        if logger.IsWarnEnabled then sprintf "Error reading message-store version: %A" err |> logger.Warn
                        Error err
                if logger.IsTraceEnabled then sprintf "Created response. Serializing..." |> logger.Trace
                let bytes = serializer.Pickle response
                if logger.IsTraceEnabled then sprintf "Writing %i bytes to stream..." bytes.Length |> logger.Trace
                let stream = client.GetStream()
                do! stream.AsyncWriteBytes bytes
            with e -> if logger.IsErrorEnabled then sprintf "Error responding to tcp-client: %A" e |> logger.Error
        }
        let writeMessage str = async {
            use client = new TcpClient()
            do! client.ConnectAsync(IPAddress.Loopback, 6866)
            use stream = client.GetStream()
            do! stream.AsyncWriteBytes (Text.Encoding.UTF8.GetBytes (str + Environment.NewLine))
            let! rawResponse = stream.AsyncReadBytes()
            if logger.IsTraceEnabled then sprintf "Got response from server: %s" (Text.Encoding.UTF8.GetString rawResponse) |> logger.Trace
        }

        let getRemoteIp (tcp: TcpClient) =
            let ep = tcp.Client.RemoteEndPoint :?> IPEndPoint
            ep.Address

        let rec listen (_:int) = async {
            if logger.IsTraceEnabled then sprintf "Waiting for clients." |> logger.Trace
            let! (client: TcpClient) = server.AcceptTcpClientAsync()
            if logger.IsInfoEnabled then $"Connection established via tcp from {client |> getRemoteIp}" |> logger.Info
            use (stream: NetworkStream) = client.GetStream()
            if logger.IsTraceEnabled then sprintf "Got stream. Waiting for bytes..." |> logger.Trace
            try
                try
                    use tokenSource = new CancellationTokenSource (connectionTimeout)
                    try
                        let! bytes = stream.AsyncReadBytes(tokenSource.Token)
                        if logger.IsTraceEnabled then sprintf "Read bytes - Size: %i" bytes.Length |> logger.Trace
                        let request = serializer.UnPickle<Request> bytes
                        if logger.IsTraceEnabled then sprintf "Unpickled request %A" request |> logger.Trace
                        match request with
                        | AppendMessage (streamName,expectedVersion,unrecordedMessage) ->
                            do! appendEvent client (streamName,expectedVersion,unrecordedMessage)
                        | AppendMessages (streamName,expectedVersion,unrecordedMessages) ->
                            do! appendEventsToStream client (streamName,expectedVersion,unrecordedMessages)
                        | ReadStreamMessages (streamName,fromEventNumber,numEvents) ->
                            do! readStreamEvents client (streamName,fromEventNumber,numEvents)
                        | ReadCategoryMessages (streamName,fromEventNumber,numEvents) ->
                            do! readStreamEvents client (streamName,fromEventNumber,numEvents)
                        | ReadMessageStoreVersion ->
                            do! readMessageStoreVersion client
                        if logger.IsInfoEnabled then $"Done! Disconnected from {client |> getRemoteIp}" |> logger.Info
                    with
                    | :? AggregateException as e when (e.InnerExceptions[0]).GetType() = typeof<OperationCanceledException> ->
                        if logger.IsInfoEnabled then $"Disconnected from {client |> getRemoteIp} after {connectionTimeout.TotalMinutes} minutes of inactivity." |> logger.Info
                    | :? Tasks.TaskCanceledException ->
                        if logger.IsInfoEnabled then $"{client |> getRemoteIp} has disconnected without any action." |> logger.Info
                with e -> if logger.IsErrorEnabled then sprintf "%A" e |> logger.Error
            finally
                client.Dispose()
                stream.Dispose()
            return! listen 1
        }

        let cts = new CancellationTokenSource()
        server.Start(int SocketOptionName.MaxConnections)
        Async.Start(listen 1, cts.Token)
        {
            new IDisposable with
                member __.Dispose() = 
                    cts.Cancel()
                    server.Stop()
        }