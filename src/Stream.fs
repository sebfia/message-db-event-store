namespace EventStore.Native

[<AutoOpen>]
module Extension =
    open System
    open System.IO
    open System.Threading.Tasks

    type AsyncBuilder with
        member __.Bind(f: Task<'T>, g : 'T -> Async<'S>) = __.Bind(Async.AwaitTask f, g)
        member __.Bind(f: Task, g : unit -> Async<'S>) = __.Bind(f.ContinueWith ignore, g)
    
    type Stream with
        member s.AsyncWriteBytes (bytes: byte array) = async {
            do! s.WriteAsync(BitConverter.GetBytes bytes.Length, 0, 4)
            do! s.WriteAsync(bytes, 0, bytes.Length)
            do! s.FlushAsync()
        }

        member s.AsyncReadBytes (length: int,?cancellationToken) = 
            let rec readSegment buf offset remaining = async {
                let! read = s.ReadAsync(buf, offset, remaining, (cancellationToken |> Option.defaultValue Threading.CancellationToken.None))
                if read < remaining then
                    return! readSegment buf (offset + read) (remaining - read)
                else return ()
            }
            async {
                let bytes = Array.zeroCreate<byte> length
                do! readSegment bytes 0 length
                return bytes
            }

        member s.AsyncReadBytes(?cancellationToken: Threading.CancellationToken) = async {
            let! firstFour = s.AsyncReadBytes (4,(cancellationToken |> Option.defaultValue Threading.CancellationToken.None))
            let length = BitConverter.ToInt32(firstFour, 0)
            return! s.AsyncReadBytes (length,(cancellationToken |> Option.defaultValue Threading.CancellationToken.None))
        }
