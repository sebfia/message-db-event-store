namespace EventStore

module Client =
    
    open EventStore.Native
    open MBrace.FsPickler
    open System.Net
    open System.Net.Sockets

    type IEventStoreClient =
            // abstract member CreateNewStream: category:string -> streamId:string -> Async<Response>
            // abstract member GetStreamsInCategory: category:string -> Async<Response>
            abstract member AppendMessage: streamName:string -> expectedVersion:int64 -> event:UnrecordedMessage -> Async<Result<RecordedMessage,MessageDbError>>
            abstract member AppendMessages: streamName:string -> expectedVersion:int64 -> events:UnrecordedMessage array -> Async<Response>
            abstract member ReadStreamMessagesForward: streamName:string -> fromVersion:int64 option -> numMessages: BatchSize -> Async<Result<(string*RecordedMessage array),MessageDbError>>
            abstract member ReadMessageStoreVersion: unit -> Async<Result<string,MessageDbError>>
            // abstract member ReadStreamEventsForwardLimited: streamName:string -> fromVersion:int64 -> numEvents:int64 -> Async<Response>
            // abstract member ReadStreamEventsBackwards: streamName:string -> fromVersion:int64 -> Async<Response>
            // abstract member ReadStreamEventsBackwardsLimited: streamName:string -> fromVersion:int64 -> numEvents:int64 -> Async<Response>

    let createClient (ip: IPAddress) (port: int) =
        let serializer = FsPickler.CreateBinarySerializer()
        let doWhileConnected createRequest translateResponse = async {
            use tcpClient = new TcpClient()
            do! tcpClient.ConnectAsync(ip, port)
            let requestBytes = createRequest()
            use stream = tcpClient.GetStream()
            do! stream.AsyncWriteBytes requestBytes
            let! responseBytes = stream.AsyncReadBytes()
            do! stream.FlushAsync()
            stream.Dispose()
            tcpClient.Close()
            return translateResponse responseBytes
        }

        let (|MessageStoreVersionRead|_|) = function
            | MessageStoreVersionRead v -> Some v
            | _ -> None

        let (|MessageAppended|_|) = function
            | MessageAppended (msg) -> Some (msg)
            | _ -> None

        let (|MessagesRead|_|) = function
            | StreamMessagesRead (n,msg) -> Some (n,msg)
            | _ -> None

        let storeVersionRead (resp: Response) =
            match resp with
            | Error e -> Error e
            | Ok x -> match x with | MessageStoreVersionRead v -> Ok v | _ -> Error InternalMessagingError
        let messageAppended (resp: Response) =
            match resp with
            | Error e -> Error e
            // | Ok x -> match x with | MessageAppended (n,v) -> Ok (n,v) | _ -> Error InternalMessagingError
            | Ok x -> match x with | MessageAppended (msg) -> Ok (msg) | _ -> Error InternalMessagingError
        let messagesRead = function
            | Error e -> Error e
            | Ok x -> match x with | MessagesRead (n,msg) -> Ok (n,msg) | _ -> Error InternalMessagingError

        let deserializeResponse buffer = serializer.UnPickle<Response> buffer
        // let createNewStream category streamId =
        //     let createRequest() = CreateStream(category, streamId) |> serializer.Pickle |> async.Return
        //     doWhileConnected createRequest translateResponse |> async.ReturnFrom
        // let getStreamsInCategory category = 
        //     let createRequest() = GetAllStreamsInCategory category |> serializer.Pickle |> async.Return
        //     doWhileConnected createRequest translateResponse |> async.ReturnFrom
        let appendMessage streamName expectedVersion message =
            let createRequest() = AppendMessage(streamName,expectedVersion,message) |> serializer.Pickle
            doWhileConnected createRequest (deserializeResponse >> messageAppended) |> async.ReturnFrom
        let appendMessages streamName expectedVersion events =
            let createRequest() = AppendMessages(streamName,expectedVersion,events) |> serializer.Pickle
            doWhileConnected createRequest deserializeResponse |> async.ReturnFrom
        let readStreamMessagesForward streamName fromVersion numMessages =
            let createRequest() = ReadStreamMessages(streamName,fromVersion, numMessages) |> serializer.Pickle
            doWhileConnected createRequest (deserializeResponse >> messagesRead) |> async.ReturnFrom
        let readMessageStoreVersion () =
            let createRequest() = ReadMessageStoreVersion |> serializer.Pickle
            doWhileConnected createRequest (deserializeResponse >> storeVersionRead) |> async.ReturnFrom
        // let readStreamEventsForwardLimited streamName fromVersion numEvents =
        //     let createRequest() = ReadStreamEventsForwardLimited(streamName,fromVersion,numEvents) |> serializer.Pickle |> async.Return
        //     doWhileConnected createRequest translateResponse |> async.ReturnFrom
        // let readStreamEventsBackwards streamName fromVersion =
        //     let createRequest() = ReadStreamEventsBackwards(streamName,fromVersion) |> serializer.Pickle |> async.Return
        //     doWhileConnected createRequest translateResponse |> async.ReturnFrom
        // let readStreamEventsBackwardsLimited streamName fromVersion numEvents =
        //     let createRequest() = ReadStreamEventsBackwardsLimited(streamName,fromVersion,numEvents) |> serializer.Pickle |> async.Return
        //     doWhileConnected createRequest translateResponse |> async.ReturnFrom
        {
            new IEventStoreClient with
                // member x.CreateNewStream category streamId = createNewStream category streamId
                // member x.GetStreamsInCategory category = getStreamsInCategory category
                member x.AppendMessage streamName expectedVersion unrecordedMessage = appendMessage streamName expectedVersion unrecordedMessage
                member x.AppendMessages streamName expectedVersion unrecordedMessages = appendMessages streamName expectedVersion unrecordedMessages
                member x.ReadStreamMessagesForward streamName fromVersion numMessages = readStreamMessagesForward streamName fromVersion numMessages
                member x.ReadMessageStoreVersion () = readMessageStoreVersion()
                // member x.ReadStreamEventsForwardLimited streamName fromVersion numEvents = readStreamEventsForwardLimited streamName fromVersion numEvents
                // member x.ReadStreamEventsBackwards streamName fromVersion = readStreamEventsBackwards streamName fromVersion
                // member x.ReadStreamEventsBackwardsLimited streamName fromVersion numEvents = readStreamEventsBackwardsLimited streamName fromVersion numEvents
        }