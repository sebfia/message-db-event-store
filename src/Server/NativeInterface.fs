namespace EventStore.Native

module TcpServer =
    open System
    open System.IO
    open System.Net
    open System.Net.Sockets
    open System.Threading
    open System.Threading.Tasks
    open MBrace.FsPickler
    open EventStore.Native.Contracts
    open EventStore
    open Contracts
    open NLog

    type DateTime with
        static member private epoch = DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)

        member x.ToEpoch () = (x - DateTime.epoch).TotalSeconds |> Convert.ToInt64

        static member FromEpoch (epoch: int64) = epoch |> Convert.ToDouble |> DateTime.epoch.AddSeconds

    type AsyncBuilder with
        member __.Bind(f: Task<'T>, g : 'T -> Async<'S>) = __.Bind(Async.AwaitTask f, g)

        member __.Bind(f: Task, g : unit -> Async<'S>) = __.Bind(f.ContinueWith ignore, g)

    type Stream with
        member s.AsyncWriteBytes (bytes: byte array) = async {
            do! s.WriteAsync(BitConverter.GetBytes bytes.Length, 0, 4)
            do! s.WriteAsync(bytes, 0, bytes.Length)
            do! s.FlushAsync()
        }

        member s.AsyncReadBytes (length: int) = 
            let rec readSegment buf offset remaining = async {
                let! read = s.ReadAsync(buf, offset, remaining)
                if read < remaining then
                    return! readSegment buf (offset + read) (remaining - read)
                else return ()
            }
            async {
                let bytes = Array.zeroCreate<byte> length
                do! readSegment bytes 0 length
                return bytes
            }

        member s.AsyncReadBytes() = async {
            let! firstFour = s.AsyncReadBytes 4
            let length = BitConverter.ToInt32(firstFour, 0)
            return! s.AsyncReadBytes length
        }
    let logger = NLog.LogManager.GetLogger("Tcp-Server")
    let start (connectionString: string) (endpoint: IPEndPoint) =
        if logger.IsDebugEnabled then sprintf "Starting native interface with connection string: %s" connectionString |> logger.Debug
        let serializer = FsPickler.CreateBinarySerializer()
        let server = TcpListener(endpoint)
        let messageStore = Engine.MessageStore(connectionString)
        
        let appendEvent (client: TcpClient) (streamName,expectedVersion,(unrecordedMessage: UnrecordedMessage)) = async {
            try
                let! result = messageStore.WriteStreamMessage(streamName, unrecordedMessage, expectedVersion)
                let response =
                    match result  with
                    | Error exn -> 
                        if logger.IsWarnEnabled then sprintf "Error appending event to stream: %s%s%A" streamName Environment.NewLine exn |> logger.Warn
                        Error (sprintf "%A" exn)
                    | Ok num -> Ok (MessageAppended (streamName, num))
                let bytes = serializer.Pickle response
                let stream = client.GetStream()
                do! stream.AsyncWriteBytes bytes
            with e -> if logger.IsErrorEnabled then sprintf "Error responding to tcp-client: %A" e |> logger.Error
        }
        let appendEventsToStream (client: TcpClient) (streamName,expectedEventNumber,(unrecordedMessages: UnrecordedMessage array)) = async {
            try
                let! result = messageStore.WriteStreamMessages(streamName, unrecordedMessages, expectedEventNumber)
                let response =
                    match result with
                    | Ok nums -> Ok (MessagesAppended(streamName,nums |> List.max))
                    | Error exn ->
                        if logger.IsWarnEnabled then sprintf "Error appending events to stream: %s%s%A" streamName Environment.NewLine exn |> logger.Warn
                        Error (sprintf "%A" exn)
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
                    | Error exn ->
                        if logger.IsWarnEnabled then sprintf "Error reading events from stream: %s%s%A" streamName Environment.NewLine exn |> logger.Warn
                        Error (sprintf "%A" exn)
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

        let rec listen (_:int) = async {
            if logger.IsTraceEnabled then sprintf "Waiting for clients." |> logger.Trace
            let! (client: TcpClient) = server.AcceptTcpClientAsync()
            if logger.IsTraceEnabled then sprintf "Got tcp client." |> logger.Trace
            let (stream: NetworkStream) = client.GetStream()
            if logger.IsTraceEnabled then sprintf "Got stream." |> logger.Trace
            let! bytes = stream.AsyncReadBytes()
            if logger.IsTraceEnabled then sprintf "Read bytes - Size: %i" bytes.Length |> logger.Trace
            try
                let request = serializer.UnPickle<Request> bytes
                if logger.IsTraceEnabled then sprintf "Unpickled request %A" request |> logger.Trace
                match request with
                | AppendMessage (streamName,expectedEventNumber,unrecordedMessage) ->
                    appendEvent client (streamName,expectedEventNumber,unrecordedMessage) |> Async.Start
                | AppendMessages (streamName,expectedEventNumber,unrecordedMessages) ->
                    appendEventsToStream client (streamName,expectedEventNumber,unrecordedMessages) |> Async.Start
                | ReadStreamMessages (streamName,fromEventNumber,numEvents) ->
                    readStreamEvents client (streamName,fromEventNumber,numEvents) |> Async.Start
                | ReadCategoryMessages (streamName,fromEventNumber,numEvents) ->
                    readStreamEvents client (streamName,fromEventNumber,numEvents) |> Async.Start
            with e -> if logger.IsErrorEnabled then sprintf "%A" e |> logger.Error
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