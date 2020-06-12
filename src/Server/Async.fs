module Async
open System
open System.Threading.Tasks
#nowarn "40"

let internal synchronize f = 
  let ctx = System.Threading.SynchronizationContext.Current 
  f (fun g ->
    let nctx = System.Threading.SynchronizationContext.Current 
    if ctx <> null && ctx <> nctx then ctx.Post((fun _ -> g()), null)
    else g() )

    /// Creates an asynchronous workflow that will be resumed when the 
    /// specified observables produces a value. The workflow will return 
    /// the value produced by the observable.
let AwaitObservable(ev1:IObservable<'T1>) =
    synchronize (fun f ->
      Async.FromContinuations((fun (cont,econt,ccont) -> 
        let called = ref false
        let rec finish cont value = 
          remover.Dispose()
          f (fun () -> lock called (fun () -> 
              if not called.Value then
                 cont value
                 called.Value <- true) )
        and remover : IDisposable = 
          ev1.Subscribe
            ({ new IObserver<_> with
                  member x.OnNext(v) = finish cont v
                  member x.OnError(e) = finish econt e
                  member x.OnCompleted() = 
                    let msg = "Cancelling the workflow, because the Observable awaited using AwaitObservable has completed."
                    finish ccont (new System.OperationCanceledException(msg)) }) 
        () )))
let ForEach<'T> (computations: Async<'T> list) =
    let rec forEach l r =
        async {
            match l with
            | f::t -> 
                let! x = f
                return! x::r |> forEach t
            | [] -> return r |> List.rev
        }
    async {
        return! forEach computations []
    }
let StartAsPlainTask (work : Async<unit>) = Task.Factory.StartNew(fun () -> work |> Async.RunSynchronously)