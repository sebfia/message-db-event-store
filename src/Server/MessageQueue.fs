(*
#if INTERACTIVE
#r "../../packages/spiking/NATS.Client/lib/net45/NATS.Client.dll"
#r "../../packages/spiking/Polly/lib/net461/Polly.dll"
#r "../../packages/spiking/NLog/lib/net45/NLog.dll"
#load "Agent.fs"
#load "Async.fs"
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
        let delay ms f =
            async {
                do! Async.Sleep ms
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
                                                s.Start |> delay 50
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