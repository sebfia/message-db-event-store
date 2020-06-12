module Agent

let startStatelessAgent processMessage =
  MailboxProcessor.Start(fun inbox ->
    let rec loop _ =
      async {
        let! msg = inbox.Receive()
        do! processMessage msg
        return! loop true
      }
    loop true)
    
let createStatelessAgent<'TMessage> processMessage =
    new MailboxProcessor<'TMessage>(fun inbox ->
        let rec loop _ =
            async {
                let! msg = inbox.Receive()
                do! processMessage msg
                return! loop true
            }
        loop true)

let startStatefulAgent<'TMessage, 'TState>  (initialState: 'TState) (processMessage: 'TState -> 'TMessage -> Async<'TState>) =
  MailboxProcessor.Start(fun inbox ->
    let rec loop state =
      async {
        let! msg = inbox.Receive()
        let! newState = processMessage state msg
        return! loop newState
      }
    loop initialState)

let startStatefulAgent'<'TMessage, 'TState> (initialState: 'TState) (processMessage: 'TState -> 'TMessage -> Async<'TState>) =
  let mutable state = initialState
  MailboxProcessor.Start(fun inbox ->
    let rec loop _ =
      async {
        let! msg = inbox.Receive()
        let! newState = processMessage state msg
        state <- newState
        return! loop true
      }
    loop true)
let become state = async.Return state
