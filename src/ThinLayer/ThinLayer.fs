namespace EventStore

module Sql =
    open System
    open Npgsql
    open System.Data
    open System.Threading
    open System.Collections.Generic

    type RowReader(reader: NpgsqlDataReader) =
        let columnDict = Dictionary<string, int>()
        let columnTypes = Dictionary<string, string>()
        do
            // Populate the names of the columns into a dictionary
            // such that each read doesn't need to loop through all columns
            for fieldIndex in [0 .. reader.FieldCount - 1] do
                columnDict.Add(reader.GetName(fieldIndex), fieldIndex)
                columnTypes.Add(reader.GetName(fieldIndex), reader.GetDataTypeName(fieldIndex))

        let failToRead (column: string) (columnType: string) =
            let availableColumns =
                columnDict.Keys
                |> Seq.map (fun key -> sprintf "[%s:%s]" key columnTypes.[key])
                |> String.concat ", "

            failwithf "Could not read column '%s' as %s. Available columns are %s"  column columnType availableColumns
        with
        
        member this.int64(column: string) : int64 =
            match columnDict.TryGetValue(column) with
            | true, columnIndex -> reader.GetInt64(columnIndex)
            | false, _ -> failToRead column "int64"

        member this.int64OrNone(column: string) : int64 option =
            match columnDict.TryGetValue(column) with
            | true, columnIndex ->
                if reader.IsDBNull(columnIndex)
                then None
                else Some (reader.GetInt64(columnIndex))
            | false, _ -> failToRead column "int64"

        member this.string(column: string) : string =
            match columnDict.TryGetValue(column) with
            | true, columnIndex -> reader.GetString(columnIndex)
            | false, _ -> failToRead column "string"

        member this.stringOrNone(column: string) : string option =
            match columnDict.TryGetValue(column) with
            | true, columnIndex ->
                if reader.IsDBNull(columnIndex)
                then None
                else Some (reader.GetString(columnIndex))
            | false, _ -> failToRead column "string"

        member this.bool(column: string) : bool =
            match columnDict.TryGetValue(column) with
            | true, columnIndex -> reader.GetFieldValue<bool>(columnIndex)
            | false, _ -> failToRead column "bool"

        member this.boolOrNone(column: string) : bool option =
            match columnDict.TryGetValue(column) with
            | true, columnIndex ->
                if reader.IsDBNull(columnIndex)
                then None
                else Some (reader.GetFieldValue<bool>(columnIndex))
            | false, _ -> failToRead column "bool"

        member this.timestamp(column: string) =
            match columnDict.TryGetValue(column) with
            | true, columnIndex -> reader.GetDateTime(columnIndex)
            | false, _ -> failToRead column "timestamp"

        member this.timestampOrNone(column: string) =
            match columnDict.TryGetValue(column) with
            | true, columnIndex ->
                if reader.IsDBNull(columnIndex)
                then None
                else Some (reader.GetTimeStamp(columnIndex))
            | false, _ -> failToRead column "timestamp"

        member this.timestamptz(column: string) =
            match columnDict.TryGetValue(column) with
            | true, columnIndex -> reader.GetTimeStamp(columnIndex)
            | false, _ -> failToRead column "timestamp"

        member this.timestamptzOrNone(column: string) =
            match columnDict.TryGetValue(column) with
            | true, columnIndex ->
                if reader.IsDBNull(columnIndex)
                then None
                else Some (reader.GetTimeStamp(columnIndex))
            | false, _ -> failToRead column "timestamp"

        /// Gets the value of the specified column as a globally-unique identifier (GUID).
        member this.uuid(column: string) : Guid =
            match columnDict.TryGetValue(column) with
            | true, columnIndex -> reader.GetGuid(columnIndex)
            | false, _ -> failToRead column "guid"

        member this.uuidOrNone(column: string) : Guid option =
            match columnDict.TryGetValue(column) with
            | true, columnIndex ->
                if reader.IsDBNull(columnIndex)
                then None
                else Some(reader.GetGuid(columnIndex))
            | false, _ -> failToRead column "guid"

        /// Gets the value of the specified column as a globally-unique identifier (GUID).
        member this.uuidArray(column: string) : Guid [] =
            match columnDict.TryGetValue(column) with
            | true, columnIndex -> reader.GetFieldValue<Guid []>(columnIndex)
            | false, _ -> failToRead column "guid[]"

        member this.uuidArrayOrNone(column: string) : Guid [] option =
            match columnDict.TryGetValue(column) with
            | true, columnIndex ->
                if reader.IsDBNull(columnIndex)
                then None
                else Some(reader.GetFieldValue<Guid []>(columnIndex))
            | false, _ -> failToRead column "guid[]"

        /// Gets the value of the specified column as a System.DateTime object.
        member this.dateTime(column: string) : DateTime =
            match columnDict.TryGetValue(column) with
            | true, columnIndex -> reader.GetDateTime(columnIndex)
            | false, _ -> failToRead column "datetime"

        /// Gets the value of the specified column as a System.DateTime object.
        member this.dateTimeOrNone(column: string) : DateTime option =
            match columnDict.TryGetValue(column) with
            | true, columnIndex ->
                if reader.IsDBNull(columnIndex)
                then None
                else Some (reader.GetDateTime(columnIndex))
            | false, _ -> failToRead column "datetime"

        member this.bytea(column: string) : byte[] =
            match columnDict.TryGetValue(column) with
            | true, columnIndex -> reader.GetFieldValue<byte[]>(columnIndex)
            | false, _ -> failToRead column "byte[]"

        /// Reads the specified column as `byte[]`
        member this.byteaOrNone(column: string) : byte[] option =
            match columnDict.TryGetValue(column) with
            | true, columnIndex ->
                if reader.IsDBNull(columnIndex)
                then None
                else Some (reader.GetFieldValue<byte[]>(columnIndex))
            | false, _ -> failToRead column "byte[]"
        
    type SqlType = Varchar | Bigint | Jsonb

    let createParameter paramType (name: string) value =
        let typeToNpgsql = function
            | Varchar -> NpgsqlTypes.NpgsqlDbType.Varchar
            | Bigint -> NpgsqlTypes.NpgsqlDbType.Bigint
            | Jsonb -> NpgsqlTypes.NpgsqlDbType.Jsonb
        let normalize (p: string) = if not (p.StartsWith("@")) then (sprintf "@%s" p) else p
        NpgsqlParameter(name |> normalize, parameterType = (typeToNpgsql paramType), Value = (box value))
    let stringParameter name (value: string) = createParameter Varchar name value
    let int64Parameter name (value: int64) = createParameter Bigint name value
    let jsonParameter name (value: string) = createParameter Jsonb name value

    let createFunc (connection: NpgsqlConnection) query parameters =
        let cmd = new NpgsqlCommand(query, connection)
        cmd.CommandType <- CommandType.StoredProcedure
        parameters |> List.map (fun (p: NpgsqlParameter) -> cmd.Parameters.Add(p)) |> ignore
        cmd

    let rec While<'T> (predicate: Async<bool>) (action: Async<'T>) (initial: 'T list) : Async<'T list> = async {
        let! b = predicate
        if b then 
            let! x = action
            return! While predicate action (x::initial)
        else return initial
    }

    let executeQueryAsync<'T> (connection: NpgsqlConnection) (command: NpgsqlCommand) cancellationToken (f: RowReader -> 'T) = async {
        let! tk = Async.CancellationToken
        use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(tk, cancellationToken)
        let mergedToken = mergedTokenSource.Token
        try
            if not (connection.State.HasFlag ConnectionState.Open) then do! Async.AwaitTask (connection.OpenAsync mergedToken)
            let! reader = command.ExecuteReaderAsync mergedToken |> Async.AwaitTask
            let rowReader = RowReader(reader)
            let result = ResizeArray<'T>()
            while reader.Read() do
                f rowReader |> result.Add
            do! reader.CloseAsync() |> Async.AwaitTask
            reader.Dispose()
            return Ok (List.ofSeq result)
        with 
        :? AggregateException as e -> return Error (e.InnerExceptions |> Seq.head)
        | e -> return Error e
    }
        
    let executeScalarAsync<'T> (connection: NpgsqlConnection) (commands: NpgsqlCommand array) cancellationToken = async {
        let! tk = Async.CancellationToken
        use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(tk, cancellationToken)
        let mergedToken = mergedTokenSource.Token
        if Array.isEmpty commands then return Ok []
        else
        try
            if not (connection.State.HasFlag ConnectionState.Open) then do! Async.AwaitTask (connection.OpenAsync mergedToken)
            use tx = connection.BeginTransaction()
            let results = ResizeArray<'T>()
            for cmd in commands do
                let! o = cmd.ExecuteScalarAsync(mergedToken) |> Async.AwaitTask
                o |> unbox |> results.Add
            do! tx.CommitAsync mergedToken |> Async.AwaitTask
            return List.ofSeq results |> Ok
        with 
        :? AggregateException as e -> return Error (e.InnerExceptions |> Seq.head)
        | e -> return Error e
    }

    let setRole (connection: NpgsqlConnection) cancellationToken roleName = async {
        let! tk = Async.CancellationToken
        use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(tk, cancellationToken)
        let mergedToken = mergedTokenSource.Token
        if not (connection.State.HasFlag ConnectionState.Open) then do! Async.AwaitTask (connection.OpenAsync mergedToken)
        use cmd = new NpgsqlCommand((sprintf "SET ROLE %s" roleName), connection)
        return! Async.AwaitTask (cmd.ExecuteNonQueryAsync(mergedToken))
    }
