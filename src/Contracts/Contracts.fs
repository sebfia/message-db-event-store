namespace EventStore.Native

[<AutoOpen>]
module Contracts =
    open System
    type RecordedMessage = 
        {
            Id: Guid
            StreamName : string
            CreatedTimeUTC: DateTime
            Version: int64
            EventType : string
            Metadata : string option
            Data : string
        }
    type UnrecordedMessage = {
        Id: Guid
        EventType: string
        Metadata: string option
        Data: string
    }
    type BatchSize = All | Limited of int64
    type SuccessResponse =
        | MessageAppended of streamName:string*messageNumber:int64
        | MessagesAppended of streamName:string*lastMessageNumber:int64
        | StreamMessagesRead of streamName:string*recordedMessages:RecordedMessage array
        | CategoryMessagesRead of categoryName:string*recordedMessages: RecordedMessage array
    type Response = Result<SuccessResponse,string>
    type Request =
        | AppendMessage of streamName:string*expectedVersion:int64*unrecordedEvent:UnrecordedMessage
        | AppendMessages of streamName:string*expectedEventNumber:int64*unrecordedEvents:UnrecordedMessage array
        | ReadStreamMessages of streamName:string*fromEventNumber:int64 option*numEvents:BatchSize
        | ReadCategoryMessages of categoryName:string*fromEventNumber:int64 option*numEvents:BatchSize