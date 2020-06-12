namespace EventStore
[<RequireQualifiedAccess>]
module RuntimeEnvironment =
    open System.IO
    let private envPath = "/.dockerenv"
    let private procPath = "/proc/self/cgroup"
    let private directoryExists dir = Directory.Exists(dir)
    let private loadFile path = 
        if File.Exists(path) then File.ReadAllText(path) |> Some
        else None
    let private containsText (text: string) (content: string) =
        content.Contains(text)
    let private executingAssemblyPath() =
        System.Reflection.Assembly.GetExecutingAssembly() |> fun a -> Path.GetDirectoryName(a.Location)
    type Context =
        {
            ConfigurationPath: string
            AppPath: string
        }
        with 
            static member Local = 
                let localPath = executingAssemblyPath()
                { ConfigurationPath = localPath; AppPath = localPath }
            static member Docker =
                { ConfigurationPath = "/config"; AppPath = "/app" }
    type RunMode =
        | Cmd of Context
        | Container of Context
    let getRunMode () =
        if loadFile procPath |> Option.map (containsText "docker") |> Option.defaultValue false
            || directoryExists envPath then
            Container Context.Docker
        else
            Cmd Context.Local
