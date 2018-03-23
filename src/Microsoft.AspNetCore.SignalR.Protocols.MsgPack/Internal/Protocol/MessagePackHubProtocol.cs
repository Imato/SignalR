// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.SignalR.Internal.Formatters;
using Microsoft.Extensions.Options;
using MessagePack;

namespace Microsoft.AspNetCore.SignalR.Internal.Protocol
{
    public class MessagePackHubProtocol : IHubProtocol
    {
        private const int ErrorResult = 1;
        private const int VoidResult = 2;
        private const int NonVoidResult = 3;

        public static readonly string ProtocolName = "messagepack";
        public static readonly int ProtocolVersion = 1;

        private static bool _setup;

        public string Name => ProtocolName;

        public int Version => ProtocolVersion;

        public TransferFormat TransferFormat => TransferFormat.Binary;

        public MessagePackHubProtocol()
            : this(Options.Create(new MessagePackHubProtocolOptions()))
        { }

        public MessagePackHubProtocol(IOptions<MessagePackHubProtocolOptions> options)
        {
            if (!_setup)
            {
                MessagePack.Resolvers.CompositeResolver.RegisterAndSetAsDefault(
                    // Resolve DateTime first
                    MessagePack.Resolvers.NativeDateTimeResolver.Instance,
                    MessagePack.Resolvers.ContractlessStandardResolver.Instance
                );
                _setup = true;
            }
        }

        public bool IsVersionSupported(int version)
        {
            return version == Version;
        }

        public bool TryParseMessages(ReadOnlyMemory<byte> input, IInvocationBinder binder, IList<HubMessage> messages)
        {
            while (BinaryMessageParser.TryParseMessage(ref input, out var payload))
            {
                var isArray = MemoryMarshal.TryGetArray(payload, out var arraySegment);
                // This will never be false unless we started using un-managed buffers
                Debug.Assert(isArray);
                var message = ParseMessage(arraySegment.Array, arraySegment.Offset, binder);
                if (message != null)
                {
                    messages.Add(message);
                }
            }

            return messages.Count > 0;
        }

        private static HubMessage ParseMessage(byte[] input, int startOffset, IInvocationBinder binder)
        {
            _ = MessagePackBinary.ReadArrayHeader(input, startOffset, out var readSize);
            startOffset += readSize;

            var messageType = ReadInt32(input, ref startOffset, "messageType");

            switch (messageType)
            {
                case HubProtocolConstants.InvocationMessageType:
                    return CreateInvocationMessage(input, ref startOffset, binder);
                case HubProtocolConstants.StreamInvocationMessageType:
                    return CreateStreamInvocationMessage(input, ref startOffset, binder);
                case HubProtocolConstants.StreamItemMessageType:
                    return CreateStreamItemMessage(input, ref startOffset, binder);
                case HubProtocolConstants.CompletionMessageType:
                    return CreateCompletionMessage(input, ref startOffset, binder);
                case HubProtocolConstants.CancelInvocationMessageType:
                    return CreateCancelInvocationMessage(input, ref startOffset);
                case HubProtocolConstants.PingMessageType:
                    return PingMessage.Instance;
                case HubProtocolConstants.CloseMessageType:
                    return CreateCloseMessage(input, ref startOffset);
                default:
                    // Future protocol changes can add message types, old clients can ignore them
                    return null;
            }
        }

        private static InvocationMessage CreateInvocationMessage(byte[] input, ref int offset, IInvocationBinder binder)
        {
            var headers = ReadHeaders(input, ref offset);
            var invocationId = ReadInvocationId(input, ref offset);

            // For MsgPack, we represent an empty invocation ID as an empty string,
            // so we need to normalize that to "null", which is what indicates a non-blocking invocation.
            if (string.IsNullOrEmpty(invocationId))
            {
                invocationId = null;
            }

            var target = ReadString(input, ref offset, "target");
            var parameterTypes = binder.GetParameterTypes(target);

            try
            {
                var arguments = BindArguments(input, ref offset, parameterTypes);
                return ApplyHeaders(headers, new InvocationMessage(invocationId, target, argumentBindingException: null, arguments: arguments));
            }
            catch (Exception ex)
            {
                return ApplyHeaders(headers, new InvocationMessage(invocationId, target, ExceptionDispatchInfo.Capture(ex)));
            }
        }

        private static StreamInvocationMessage CreateStreamInvocationMessage(byte[] input, ref int offset, IInvocationBinder binder)
        {
            var headers = ReadHeaders(input, ref offset);
            var invocationId = ReadInvocationId(input, ref offset);
            var target = ReadString(input, ref offset, "target");
            var parameterTypes = binder.GetParameterTypes(target);

            try
            {
                var arguments = BindArguments(input, ref offset, parameterTypes);
                return ApplyHeaders(headers, new StreamInvocationMessage(invocationId, target, argumentBindingException: null, arguments: arguments));
            }
            catch (Exception ex)
            {
                return ApplyHeaders(headers, new StreamInvocationMessage(invocationId, target, ExceptionDispatchInfo.Capture(ex)));
            }
        }

        private static StreamItemMessage CreateStreamItemMessage(byte[] input, ref int offset, IInvocationBinder binder)
        {
            var headers = ReadHeaders(input, ref offset);
            var invocationId = ReadInvocationId(input, ref offset);
            var itemType = binder.GetReturnType(invocationId);
            var value = DeserializeObject(input, ref offset, itemType, "item");
            return ApplyHeaders(headers, new StreamItemMessage(invocationId, value));
        }

        private static CompletionMessage CreateCompletionMessage(byte[] input, ref int offset, IInvocationBinder binder)
        {
            var headers = ReadHeaders(input, ref offset);
            var invocationId = ReadInvocationId(input, ref offset);
            var resultKind = ReadInt32(input, ref offset, "resultKind");

            string error = null;
            object result = null;
            var hasResult = false;

            switch (resultKind)
            {
                case ErrorResult:
                    error = ReadString(input, ref offset, "error");
                    break;
                case NonVoidResult:
                    var itemType = binder.GetReturnType(invocationId);
                    result = DeserializeObject(input, ref offset, itemType, "argument");
                    hasResult = true;
                    break;
                case VoidResult:
                    hasResult = false;
                    break;
                default:
                    throw new FormatException("Invalid invocation result kind.");
            }

            return ApplyHeaders(headers, new CompletionMessage(invocationId, error, result, hasResult));
        }

        private static CancelInvocationMessage CreateCancelInvocationMessage(byte[] input, ref int offset)
        {
            var headers = ReadHeaders(input, ref offset);
            var invocationId = ReadInvocationId(input, ref offset);
            return ApplyHeaders(headers, new CancelInvocationMessage(invocationId));
        }

        private static CloseMessage CreateCloseMessage(byte[] input, ref int offset)
        {
            var error = ReadString(input, ref offset, "error");
            return new CloseMessage(error);
        }

        private static Dictionary<string, string> ReadHeaders(byte[] input, ref int offset)
        {
            var headerCount = ReadMapLength(input, ref offset, "headers");
            if (headerCount > 0)
            {
                // If headerCount is larger than int.MaxValue, things are going to go horribly wrong anyway :)
                var headers = new Dictionary<string, string>((int)headerCount);

                for (var i = 0; i < headerCount; i++)
                {
                    var key = ReadString(input, ref offset, $"headers[{i}].Key");
                    var value = ReadString(input, ref offset, $"headers[{i}].Value");
                    headers[key] = value;
                }
                return headers;
            }
            else
            {
                return null;
            }
        }

        private static object[] BindArguments(byte[] input, ref int offset, IReadOnlyList<Type> parameterTypes)
        {
            var argumentCount = ReadArrayLength(input, ref offset, "arguments");

            if (parameterTypes.Count != argumentCount)
            {
                throw new FormatException(
                    $"Invocation provides {argumentCount} argument(s) but target expects {parameterTypes.Count}.");
            }

            try
            {
                var arguments = new object[argumentCount];
                for (var i = 0; i < argumentCount; i++)
                {
                    arguments[i] = DeserializeObject(input, ref offset, parameterTypes[i], "argument");
                }

                return arguments;
            }
            catch (Exception ex)
            {
                throw new FormatException("Error binding arguments. Make sure that the types of the provided values match the types of the hub method being invoked.", ex);
            }
        }

        private static T ApplyHeaders<T>(IDictionary<string, string> source, T destination) where T : HubInvocationMessage
        {
            if (source != null && source.Count > 0)
            {
                destination.Headers = source;
            }

            return destination;
        }

        public void WriteMessage(HubMessage message, Stream output)
        {
            // We're writing data into the memoryStream so that we can get the length prefix
            using (var memoryStream = new MemoryStream())
            {
                WriteMessageCore(message, memoryStream);
                if (memoryStream.TryGetBuffer(out var buffer))
                {
                    // Write the buffer directly
                    BinaryMessageFormatter.WriteLengthPrefix(buffer.Count, output);
                    output.Write(buffer.Array, buffer.Offset, buffer.Count);
                }
                else
                {
                    BinaryMessageFormatter.WriteLengthPrefix(memoryStream.Length, output);
                    memoryStream.Position = 0;
                    memoryStream.CopyTo(output);
                }
            }
        }

        private void WriteMessageCore(HubMessage message, Stream packer)
        {
            switch (message)
            {
                case InvocationMessage invocationMessage:
                    WriteInvocationMessage(invocationMessage, packer);
                    break;
                case StreamInvocationMessage streamInvocationMessage:
                    WriteStreamInvocationMessage(streamInvocationMessage, packer);
                    break;
                case StreamItemMessage streamItemMessage:
                    WriteStreamingItemMessage(streamItemMessage, packer);
                    break;
                case CompletionMessage completionMessage:
                    WriteCompletionMessage(completionMessage, packer);
                    break;
                case CancelInvocationMessage cancelInvocationMessage:
                    WriteCancelInvocationMessage(cancelInvocationMessage, packer);
                    break;
                case PingMessage pingMessage:
                    WritePingMessage(pingMessage, packer);
                    break;
                case CloseMessage closeMessage:
                    WriteCloseMessage(closeMessage, packer);
                    break;
                default:
                    throw new FormatException($"Unexpected message type: {message.GetType().Name}");
            }
        }

        private void WriteInvocationMessage(InvocationMessage message, Stream packer)
        {
            MessagePackBinary.WriteArrayHeader(packer, 5);
            MessagePackBinary.WriteInt32(packer, HubProtocolConstants.InvocationMessageType);
            PackHeaders(packer, message.Headers);
            if (string.IsNullOrEmpty(message.InvocationId))
            {
                MessagePackBinary.WriteNil(packer);
            }
            else
            {
                MessagePackBinary.WriteString(packer, message.InvocationId);
            }
            MessagePackBinary.WriteString(packer, message.Target);
            MessagePackSerializer.Serialize(packer, message.Arguments);
        }

        private void WriteStreamInvocationMessage(StreamInvocationMessage message, Stream packer)
        {
            MessagePackBinary.WriteArrayHeader(packer, 5);
            MessagePackBinary.WriteInt16(packer, HubProtocolConstants.StreamInvocationMessageType);
            PackHeaders(packer, message.Headers);
            MessagePackBinary.WriteString(packer, message.InvocationId);
            MessagePackBinary.WriteString(packer, message.Target);
            MessagePackSerializer.Serialize(packer, message.Arguments);
        }

        private void WriteStreamingItemMessage(StreamItemMessage message, Stream packer)
        {
            MessagePackBinary.WriteArrayHeader(packer, 4);
            MessagePackBinary.WriteInt16(packer, HubProtocolConstants.StreamItemMessageType);
            PackHeaders(packer, message.Headers);
            MessagePackBinary.WriteString(packer, message.InvocationId);
            MessagePackSerializer.Serialize(packer, message.Item);
        }

        private void WriteCompletionMessage(CompletionMessage message, Stream packer)
        {
            var resultKind =
                message.Error != null ? ErrorResult :
                message.HasResult ? NonVoidResult :
                VoidResult;

            MessagePackBinary.WriteArrayHeader(packer, 4 + (resultKind != VoidResult ? 1 : 0));
            MessagePackBinary.WriteInt32(packer, HubProtocolConstants.CompletionMessageType);
            PackHeaders(packer, message.Headers);
            MessagePackBinary.WriteString(packer, message.InvocationId);
            MessagePackBinary.WriteInt32(packer, resultKind);
            switch (resultKind)
            {
                case ErrorResult:
                    MessagePackBinary.WriteString(packer, message.Error);
                    break;
                case NonVoidResult:
                    MessagePackSerializer.Serialize(packer, message.Result);
                    break;
            }
        }

        private void WriteCancelInvocationMessage(CancelInvocationMessage message, Stream packer)
        {
            MessagePackBinary.WriteArrayHeader(packer, 3);
            MessagePackBinary.WriteInt16(packer, HubProtocolConstants.CancelInvocationMessageType);
            PackHeaders(packer, message.Headers);
            MessagePackBinary.WriteString(packer, message.InvocationId);
        }

        private void WriteCloseMessage(CloseMessage message, Stream packer)
        {
            MessagePackBinary.WriteArrayHeader(packer, 2);
            MessagePackBinary.WriteInt16(packer, HubProtocolConstants.CloseMessageType);
            if (string.IsNullOrEmpty(message.Error))
            {
                MessagePackBinary.WriteNil(packer);
            }
            else
            {
                MessagePackBinary.WriteString(packer, message.Error);
            }
        }

        private void WritePingMessage(PingMessage pingMessage, Stream packer)
        {
            MessagePackBinary.WriteArrayHeader(packer, 1);
            MessagePackBinary.WriteInt32(packer, HubProtocolConstants.PingMessageType);
        }

        private void PackHeaders(Stream packer, IDictionary<string, string> headers)
        {
            if (headers != null)
            {
                MessagePackBinary.WriteMapHeader(packer, headers.Count);
                if (headers.Count > 0)
                {
                    foreach (var header in headers)
                    {
                        MessagePackBinary.WriteString(packer, header.Key);
                        MessagePackBinary.WriteString(packer, header.Value);
                    }
                }
            }
            else
            {
                MessagePackBinary.WriteMapHeader(packer, 0);
            }
        }

        private static string ReadInvocationId(byte[] input, ref int offset)
        {
            return ReadString(input, ref offset, "invocationId");
        }

        private static int ReadInt32(byte[] input, ref int offset, string field)
        {
            Exception msgPackException = null;
            try
            {
                var readInt = MessagePackBinary.ReadInt32(input, offset, out var readSize);
                offset += readSize;
                return readInt;
            }
            catch (Exception e)
            {
                msgPackException = e;
            }

            throw new FormatException($"Reading '{field}' as Int32 failed.", msgPackException);
        }

        private static string ReadString(byte[] input, ref int offset, string field)
        {
            Exception msgPackException = null;
            try
            {
                var readString = MessagePackBinary.ReadString(input, offset, out var readSize);
                offset += readSize;
                return readString;
            }
            catch (Exception e)
            {
                msgPackException = e;
            }

            throw new FormatException($"Reading '{field}' as String failed.", msgPackException);
        }

        private static bool ReadBoolean(byte[] input, ref int offset, string field)
        {
            Exception msgPackException = null;
            try
            {
                var readBool = MessagePackBinary.ReadBoolean(input, offset, out var readSize);
                offset += readSize;
                return readBool;
            }
            catch (Exception e)
            {
                msgPackException = e;
            }

            throw new FormatException($"Reading '{field}' as Boolean failed.", msgPackException);
        }

        private static long ReadMapLength(byte[] input, ref int offset, string field)
        {
            Exception msgPackException = null;
            try
            {
                var readMap = MessagePackBinary.ReadMapHeader(input, offset, out var readSize);
                offset += readSize;
                return readMap;
            }
            catch (Exception e)
            {
                msgPackException = e;
            }

            throw new FormatException($"Reading map length for '{field}' failed.", msgPackException);
        }

        private static long ReadArrayLength(byte[] input, ref int offset, string field)
        {
            Exception msgPackException = null;
            try
            {
                var readArray = MessagePackBinary.ReadArrayHeader(input, offset, out var readSize);
                offset += readSize;
                return readArray;
            }
            catch (Exception e)
            {
                msgPackException = e;
            }

            throw new FormatException($"Reading array length for '{field}' failed.", msgPackException);
        }

        private static object DeserializeObject(byte[] input, ref int offset, Type type, string field)
        {
            Exception msgPackException = null;
            try
            {
                var obj = MessagePackSerializer.NonGeneric.Deserialize(type, new ArraySegment<byte>(input, offset, input.Length - offset));
                offset += MessagePackBinary.ReadNextBlock(input, offset);
                return obj;
            }
            catch (Exception ex)
            {
                msgPackException = ex;
            }

            throw new FormatException($"Deserializing object of the `{type.Name}` type for '{field}' failed.", msgPackException);
        }
    }
}
