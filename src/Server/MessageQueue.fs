(*
#if INTERACTIVE
#r "../../packages/spiking/NATS.Client/lib/net45/NATS.Client.dll"
#r "../../packages/spiking/Polly/lib/net461/Polly.dll"
#r "../../packages/spiking/NLog/lib/net45/NLog.dll"
#load "Agent.fs"
#load "Async.fs"
last auth 12.6.20
consumer key: XDKQ1RL5IR6D9J8NIQJE43NNSBBBXOQS
https://auth.tdameritrade.com/auth?response_type=code&redirect_uri=https%3A%2F%2Flocalhost&client_id=XDKQ1RL5IR6D9J8NIQJE43NNSBBBXOQS%40AMER.OAUTHAP
encoded key: Qx%2BavxmoBkRxVcwMK4gRgcd3IAbJB7Fa87ptC4%2BFTbnsWla4aZflk2gAF%2BsP3WHqtrtclhP3a58PVm8VIuzUahJkUj64Ru5Nfd4pHKmuxk0tKRHSuPJ4xlsKAURbKc3BoqHYdSwABmbxFiBhjq69DJ9gwRUMwxMspDzrWHYyEHQDeGi%2FUs968nhE2aYpB5Cr%2Bhtval9LkzzH2GYBWL8d8LsqGXdHSAeXf%2FHsC9Qbm9aJEgqD76AwkqSYLcBk88cDAoDex6zqeBDrbQD%2BA5MqHLuODjcQqVoeW%2Fm60kZso8MbJfHhv7tPJLbaHGQF%2B0rAxrHEuWeSvfNxQ43IIRzxrxUlY4u8%2BRRxW8qIGjb46GejqXAdo2O6poHNM6KHHJxpF4kpaaqBzBykGnJQ1%2F9CGBSgNbk2oQL2%2FtK%2B5FXAMD2A3Qiu1lvAbjCyce3100MQuG4LYrgoVi%2FJHHvlCSR0cCBpAa9262z73GAhus%2BtpThdXv9mU8gggxguIwBn3bia%2FssxaTBp4m0u9kR2dkDJImxx6tSA63Wg0d95F%2FRYA1PTSJVw1dH3R7QRcRRB2%2BSz9%2BS8ihdGoLAgh5MmAzP2oF1Kllj6auasCZxg2LdKxJzQWx0GmL8LI7PdgDhsHkLam%2BmFWbsvHD4OXg9w3juud03jEfcvoidrkEMv4yKvbtgObuMRAcnXDeu%2F68UVmzt829iIWwfgW3g3W8EgR%2BA1VXf%2BspE4J4ZjPDnXpJsvPu%2BRvcb0bpTiAtz18SA346rdcsSs1kccK9P7WNUL92a06a5qMAj8ko3gbRhW0mOeppXaXQsPz73eG7eOhBZ48y%2B08PJQxhO12sMDr9Oa0rU8v%2BjhlSvf2tkhA%2F98m4%2FSxe0z5qLIEQFw7vSUEuWc3FTF0ZwgYIbzvoI%3D212FD3x19z9sWBHDJACbC00B75E
decoded key: Qx+avxmoBkRxVcwMK4gRgcd3IAbJB7Fa87ptC4+FTbnsWla4aZflk2gAF+sP3WHqtrtclhP3a58PVm8VIuzUahJkUj64Ru5Nfd4pHKmuxk0tKRHSuPJ4xlsKAURbKc3BoqHYdSwABmbxFiBhjq69DJ9gwRUMwxMspDzrWHYyEHQDeGi/Us968nhE2aYpB5Cr+htval9LkzzH2GYBWL8d8LsqGXdHSAeXf/HsC9Qbm9aJEgqD76AwkqSYLcBk88cDAoDex6zqeBDrbQD+A5MqHLuODjcQqVoeW/m60kZso8MbJfHhv7tPJLbaHGQF+0rAxrHEuWeSvfNxQ43IIRzxrxUlY4u8+RRxW8qIGjb46GejqXAdo2O6poHNM6KHHJxpF4kpaaqBzBykGnJQ1/9CGBSgNbk2oQL2/tK+5FXAMD2A3Qiu1lvAbjCyce3100MQuG4LYrgoVi/JHHvlCSR0cCBpAa9262z73GAhus+tpThdXv9mU8gggxguIwBn3bia/ssxaTBp4m0u9kR2dkDJImxx6tSA63Wg0d95F/RYA1PTSJVw1dH3R7QRcRRB2+Sz9+S8ihdGoLAgh5MmAzP2oF1Kllj6auasCZxg2LdKxJzQWx0GmL8LI7PdgDhsHkLam+mFWbsvHD4OXg9w3juud03jEfcvoidrkEMv4yKvbtgObuMRAcnXDeu/68UVmzt829iIWwfgW3g3W8EgR+A1VXf+spE4J4ZjPDnXpJsvPu+Rvcb0bpTiAtz18SA346rdcsSs1kccK9P7WNUL92a06a5qMAj8ko3gbRhW0mOeppXaXQsPz73eG7eOhBZ48y+08PJQxhO12sMDr9Oa0rU8v+jhlSvf2tkhA/98m4/Sxe0z5qLIEQFw7vSUEuWc3FTF0ZwgYIbzvoI=212FD3x19z9sWBHDJACbC00B75E
#else
*)
namespace Mercator
module MessageQueue =
    open System
    open NATS.Client
    open Polly
    open NLog
    
    type QueueMessage = 
        {
            CorrelationId: string
            Subject: string
            Content: byte array
        }
        with
            static member New subject content =
                {
                    CorrelationId = Guid.NewGuid().ToString("B").ToUpper()
                    Subject = subject
                    Content = content
                }
            
    type QueueMessageHandler = QueueMessage -> Async<unit>
    type MessageQueueError =
        | SubscriptionAlreadyExists of string
        | NoSubscriptionFound of string
    type IMessageQueueListener =
        inherit IDisposable
        abstract member AddSubscription: string -> QueueMessageHandler -> Async<Result<string,MessageQueueError>>
        abstract member RemoveSubscription: string -> Async<Result<string,MessageQueueError>>
    let enqueue (url: string) =
        let conn = ConnectionFactory().CreateConnection(url)
        fun message ->
            conn.Publish(message.Subject, message.CorrelationId, message.Content)
    
    type private Subscriber =
        {
            Observer: IDisposable
            Subscription: IAsyncSubscription
            Handler: QueueMessageHandler
        }
    type private Coordinate =
        | AddSubscription of subject:string*QueueMessageHandler*AsyncReplyChannel<Result<string,MessageQueueError>>
        | RemoveSubscription of subject:string*AsyncReplyChannel<Result<string,MessageQueueError>>
        | EnqueueMessage of QueueMessage
        | MessageProcessed of int*Result<unit,exn>
    type private Process =
        | ProcessMessage of MailboxProcessor<Coordinate>*int*QueueMessage*QueueMessageHandler*IAsyncPolicy
    let private processMessage msg =
        async {
            match msg with
            | ProcessMessage (coordinate,workerId,message,handler,policy) ->
                let f() = handler message |> Async.StartAsPlainTask
                let! result = policy.ExecuteAndCaptureAsync(f) |> Async.AwaitTask
                match result.Outcome with
                | OutcomeType.Successful -> coordinate.Post(MessageProcessed(workerId,(Result.Ok ())))
                | _ -> coordinate.Post(MessageProcessed(workerId,(Result.Error result.FinalException)))

        }
    let startQueueListener (url: string) (handlingPolicy: IAsyncPolicy) =
        let log = LogManager.GetLogger("MessageQueue.Listener")
        let conn = ConnectionFactory().CreateConnection(url)
        let availableWorkers = Array.create 4 true
        let workers = Array.init 4 (fun _ -> (Agent.startStatelessAgent processMessage))
        let nextAvailableWorker() =
            match availableWorkers |> Array.mapi (fun i b -> i,b) |> Array.where (fun (_,b) -> b) |> Array.tryHead with
            | Some (i,_) -> Some (i,workers.[i])
            | _ -> None
        let delay (timespan: TimeSpan) f =
            async {
                do! Async.Sleep timespan
                do f()
            } |> Async.StartImmediate
        let controller = 
            MailboxProcessor.Start(fun inbox ->
                let rec loop (subscribers: Map<string,Subscriber>) =
                    async {
                        let! subscribers' =
                            inbox.Scan(
                                function
                                | AddSubscription (subject,handler,ch) ->
                                    Some(
                                        async {
                                            log.Debug("Adding subscription for subject {0}", subject)
                                            match subscribers |> Map.tryFind subject with
                                            | Some _ ->
                                                ch.Reply(Error (SubscriptionAlreadyExists subject))
                                                return subscribers
                                            | None ->
                                                let s = conn.SubscribeAsync(subject)
                                                let observer =
                                                    s.MessageHandler.Subscribe(fun m -> 
                                                        {
                                                            CorrelationId = m.Message.Reply
                                                            Subject = m.Message.Subject
                                                            Content = m.Message.Data
                                                        }
                                                        |> EnqueueMessage
                                                        |> inbox.Post)
                                                let subs =
                                                    subscribers
                                                    |> Map.add subject
                                                        {
                                                            Observer = observer
                                                            Subscription = s
                                                            Handler = handler
                                                        }
                                                s.Start |> delay (TimeSpan.FromMilliseconds(50))
                                                ch.Reply(Ok subject)
                                                return subs
                                        })
                                | EnqueueMessage m ->
                                    match nextAvailableWorker() with
                                    | None -> None
                                    | Some (workerId,worker) ->
                                        match subscribers |> Map.tryFind m.Subject with
                                        | None -> log.Error("No subscriber found for subject '{0}'", m.Subject); None
                                        | Some subscriber ->
                                            log.Debug("Starting worker with id {0} on message with subject '{1}'", workerId, m.Subject)
                                            Some(
                                                async {
                                                    availableWorkers.[workerId] <- false
                                                    ProcessMessage(inbox,workerId,m,subscriber.Handler,handlingPolicy) |> worker.Post
                                                    return subscribers
                                                }
                                            )
                                | MessageProcessed (workerId,result) ->
                                    Some(
                                        async {
                                            availableWorkers.[workerId] <- true
                                            match result with
                                            | Ok _ -> log.Debug("Worker with id '{0}' successfully finished processing message.", workerId)
                                            | Error ex -> log.Error(ex,"Worker with id {0} finished processing message with error", workerId)
                                            return subscribers
                                        }
                                    )
                                | RemoveSubscription (subject, ch) ->
                                    Some(
                                        async {
                                            match subscribers |> Map.tryFind subject with
                                            | None -> ch.Reply(Error (NoSubscriptionFound subject))
                                            | Some s -> 
                                                s.Observer.Dispose()
                                                s.Subscription.Dispose()
                                                ch.Reply(Ok subject)
                                            return subscribers |> Map.remove subject
                                        }
                                    )
                        )
                        return! loop subscribers'
                    }
                loop Map.empty)
        {
            new IMessageQueueListener
                with
                    member __.AddSubscription subject handler = controller.PostAndAsyncReply(fun ch -> AddSubscription(subject,handler,ch))
                    member __.RemoveSubscription subject = controller.PostAndAsyncReply(fun ch -> RemoveSubscription(subject,ch))
                    member __.Dispose() = conn.Dispose()
        }
(*Examples:  
open Polly
open System
let listener = MessageQueue.startQueueListener "nats://127.0.0.1:4222" (Policy.Handle<System.Exception>().WaitAndRetryAsync([TimeSpan.FromSeconds(2.)]))
let receive (msg: MessageQueue.QueueMessage) =
    async {
        let text = Text.Encoding.UTF8.GetString(msg.Content)
        printfn "Got message with content %s on subject %s" text msg.Subject
    }
let name = listener.AddSubscription "nirvana" receive |> Async.RunSynchronously
MessageQueue.enqueue "nats://127.0.0.1:4222" { CorrelationId = "correlate"; Subject = "nirvana"; Content = Text.Encoding.UTF8.GetBytes("Hallo Welt aus dem Orbit!") }
listener.RemoveSubscription "nirvana" |> Async.RunSynchronously
listener.Dispose()
*)