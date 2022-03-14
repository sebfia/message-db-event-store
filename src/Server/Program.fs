namespace EventStore

module Program =
    open System
    open System.IO
    open System.Net
    open System.Threading
    open System.Runtime.Loader
    open Argu
    let defaultIp = "0.0.0.0"
    let defaultPort = 9781
    
    type private Waiter = { dummy: bool }
    type LogLevel = Off | Fatal | Error | Warn | Info | Debug | Trace
    type FanoutDriver = DevNull | Nats
    let defaultFanout = DevNull
    let defaultLogLevel = Info

    type Arguments =
        | [<Unique>] [<Mandatory>] [<AltCommandLine("-c")>] [<CustomAppSettings("CONNECTION_STRING")>] ConnectionString of string
        | [<Unique>] [<AltCommandLine("-a")>] IPAddress of string
        | [<Unique>] [<AltCommandLine("-p")>] Port of int
        | [<Unique>] [<AltCommandLine("-o")>] FanoutDriver of FanoutDriver
        | [<Unique>] [<AltCommandLine("-l")>] LogLevel of LogLevel
        | [<Unique>] Nats_Url of string
    with
        interface IArgParserTemplate with
            member s.Usage =
                match s with
                | ConnectionString _ -> "specify a connection string."
                | IPAddress _ -> sprintf "[optional] specify the ip-address to listen on. Default = %s" defaultIp
                | Port _ -> sprintf "[optional] specify the port to listen on. Default = %i" defaultPort
                | FanoutDriver _ -> sprintf "[optional] specify the fanout driver for newly recorded events. Default = %s" (sprintf "%A" defaultFanout |> fun s -> s.ToLower())
                | LogLevel _ -> sprintf "[optional] specify the log-level. Default = %s" (sprintf "%A" defaultLogLevel |> fun s -> s.ToLower())
                | Nats_Url _ -> sprintf "specify the url to the nats server if fanout driver is set to nats. Eg. nats://127.0.0.1/events/{stream-name}"
    let inline (</>) left right = Path.Combine(left, right)
    let mapLogLevel = function
    | Off -> NLog.LogLevel.Off
    | Fatal -> NLog.LogLevel.Fatal
    | Error -> NLog.LogLevel.Error
    | Warn -> NLog.LogLevel.Warn
    | Info -> NLog.LogLevel.Info
    | Debug -> NLog.LogLevel.Debug
    | Trace -> NLog.LogLevel.Trace

    [<EntryPoint>]
    let main args =
        let config = NLog.Config.LoggingConfiguration()
        let consoleTarget = new NLog.Targets.ColoredConsoleTarget("console", Layout = NLog.Layouts.SimpleLayout("${logger} ${time} ${level} ${message} ${exception}"))
        config.AddTarget(consoleTarget)
        NLog.LogManager.Configuration <- config
        try
            let reader = EnvironmentVariableConfigurationReader() :> IConfigurationReader
            let parser = ArgumentParser.Create<Arguments>(programName = "Server")
            let results = parser.Parse(args, configurationReader = reader)
            let directory =
                match results.TryGetResult ConnectionString with
                | Some connStr ->
                    // if Directory.Exists directory |> not then results.Raise("specified directory does not exist!", ErrorCode.CommandLine)
                    connStr
                | _ -> results.Raise("No connection-string specified!", ErrorCode.HelpText, true)
            let ip = 
                match results.GetResult(IPAddress, defaultValue = defaultIp) |> IPAddress.TryParse with
                | (true,x) -> x
                | _ -> results.Raise("Specified ip-address is invalid!", ErrorCode.CommandLine)
            let port = results.GetResult(Port, defaultValue = defaultPort)
            let fanout = results.GetResult(FanoutDriver, defaultValue = defaultFanout)
            let logLevel = results.GetResult(LogLevel, defaultValue = defaultLogLevel) |> mapLogLevel
            config.AddRule(logLevel, Off |> mapLogLevel, consoleTarget, "*")
            let logger = NLog.LogManager.GetLogger "Program"
            logger.Info "Starting event store."
            logger.Info("Using connection-string '{0}' and listening on address tcp://{1}:{2}", directory, ip, port)
            if logger.IsInfoEnabled then sprintf "Using fanout driver: %A" fanout |> logger.Info
            
            let endpoint = IPEndPoint(ip, port)
            let waitHandle = new ManualResetEvent(false)
            match RuntimeEnvironment.getRunMode() with
            | RuntimeEnvironment.Container _ ->
                let loadContext = AssemblyLoadContext.GetLoadContext((typeof<Waiter>).GetType().Assembly)
                loadContext.add_Unloading(fun _ -> waitHandle.Set() |> ignore)
            | RuntimeEnvironment.Cmd _ ->
                Console.TreatControlCAsInput <- false
                Console.CancelKeyPress |> Observable.add (fun x -> x.Cancel <- true; waitHandle.Set() |> ignore)
            let disposable = Native.TcpServer.start directory endpoint
            logger.Info "EventStore is started."
            waitHandle.WaitOne() |> ignore
            logger.Debug "Stopping EventStore."
            disposable.Dispose()
            logger.Info "Event Store has been stopped and all native listeners have been shut down."
            0
        with
        :? ArguParseException as e ->
            printfn "%s" e.Message
            1
        | e -> 
            printfn "Unexpected error while trying to run EventStore - %A" e
            1