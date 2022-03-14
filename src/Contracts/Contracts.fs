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
    type MessageDbError =
        | WrongExpectedVersion of expectedVersion: int64 * streamName: string * streamVersion: int64
        | StreamNameIncorrect of string
        | ErrorWritingEvent of streamName: string * eventType: string
        | UnableToConnectToDb of host: string
        | TimeoutWritingNewEvent of streamName: string
        | UnexpectedException of string
        | InternalMessagingError

    type BatchSize = All | Limited of int64
    type SuccessResponse =
        | MessageAppended of streamName:string*messageNumber:int64
        | MessagesAppended of streamName:string*lastMessageNumber:int64
        | StreamMessagesRead of streamName:string*recordedMessages:RecordedMessage array
        | CategoryMessagesRead of categoryName:string*recordedMessages: RecordedMessage array
        | MessageStoreVersionRead of string
    type Response = Result<SuccessResponse,MessageDbError>
    type Request =
        | AppendMessage of streamName:string*expectedVersion:int64*unrecordedEvent:UnrecordedMessage
        | AppendMessages of streamName:string*expectedVersion:int64*unrecordedEvents:UnrecordedMessage array
        | ReadStreamMessages of streamName:string*fromVersion:int64 option*numEvents:BatchSize
        | ReadCategoryMessages of categoryName:string*fromVersion:int64 option*numEvents:BatchSize
        | ReadMessageStoreVersion