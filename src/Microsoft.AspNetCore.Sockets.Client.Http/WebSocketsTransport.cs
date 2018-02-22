// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Sockets.Client.Http;
using Microsoft.AspNetCore.Sockets.Client.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Microsoft.AspNetCore.Sockets.Client
{
    public class WebSocketsTransport : ITransport
    {
        private readonly ClientWebSocket _webSocket;
        private IDuplexPipe _application;
        private readonly ILogger _logger;
        private readonly TimeSpan _closeTimeout;

        public Task Running { get; private set; } = Task.CompletedTask;

        public TransferMode? Mode { get; private set; }

        public WebSocketsTransport()
            : this(null, null)
        {
        }

        public WebSocketsTransport(HttpOptions httpOptions, ILoggerFactory loggerFactory)
        {
            _webSocket = new ClientWebSocket();

            if (httpOptions?.Headers != null)
            {
                foreach (var header in httpOptions.Headers)
                {
                    _webSocket.Options.SetRequestHeader(header.Key, header.Value);
                }
            }

            if (httpOptions?.AccessTokenFactory != null)
            {
                _webSocket.Options.SetRequestHeader("Authorization", $"Bearer {httpOptions.AccessTokenFactory()}");
            }

            httpOptions?.WebSocketOptions?.Invoke(_webSocket.Options);


            _closeTimeout = httpOptions?.CloseTimeout ?? TimeSpan.FromSeconds(5);
            _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<WebSocketsTransport>();
        }

        public async Task StartAsync(Uri url, IDuplexPipe application, TransferMode requestedTransferMode, IConnection connection)
        {
            if (url == null)
            {
                throw new ArgumentNullException(nameof(url));
            }

            if (application == null)
            {
                throw new ArgumentNullException(nameof(application));
            }

            if (requestedTransferMode != TransferMode.Binary && requestedTransferMode != TransferMode.Text)
            {
                throw new ArgumentException("Invalid transfer mode.", nameof(requestedTransferMode));
            }

            _application = application;
            Mode = requestedTransferMode;

            _logger.StartTransport(Mode.Value);

            await Connect(url);

            // TODO: Handle TCP connection errors
            // https://github.com/SignalR/SignalR/blob/1fba14fa3437e24c204dfaf8a18db3fce8acad3c/src/Microsoft.AspNet.SignalR.Core/Owin/WebSockets/WebSocketHandler.cs#L248-L251
            Running = ProcessSocketAsync(_webSocket);
        }

        private async Task ProcessSocketAsync(WebSocket socket)
        {
            using (socket)
            {
                // Begin sending and receiving. Receiving must be started first because ExecuteAsync enables SendAsync.
                var receiving = StartReceiving(socket);
                var sending = StartSending(socket);

                // Wait for something to complete
                var trigger = await Task.WhenAny(receiving, sending);

                if (trigger == receiving)
                {
                    // _logger.WaitingForSend();

                    // If receiving finished first, it's because of a couple of reasons
                    // 1. We received a close frame, if that's the case, we should send one back
                    if (socket.State == WebSocketState.CloseReceived)
                    {
                        await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
                    }
                    else
                    {
                        // 2. If close wasn't received that means we ended because of an exception. Here the websocket is still running
                        // fine and we need to close it with an error
                        await socket.CloseOutputAsync(WebSocketCloseStatus.InternalServerError, "", CancellationToken.None);
                    }

                    // We're waiting for the application to finish and there are 2 things it could be doing
                    // 1. Waiting for application data
                    // 2. Waiting for a websocket send to complete

                    var resultTask = await Task.WhenAny(sending, Task.Delay(_closeTimeout));

                    if (resultTask != sending)
                    {
                        // _logger.CloseTimedOut();

                        // We timed out so now we're in ungraceful shutdown mode
                        // Cancel the application so that ReadAsync yields
                        _application.Input.CancelPendingRead();

                        // Abort the websocket if we're stuck in a pending send to the client
                        socket.Abort();
                    }
                }
                else
                {
                    // _logger.WaitingForClose();

                    // The websocket receive loop is still running so we attempt to graceful close by sending the client
                    // a close frame
                    await socket.CloseOutputAsync(sending.IsFaulted ? WebSocketCloseStatus.InternalServerError : WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);

                    // We're waiting on the websocket to close and there are 2 things it could be doing
                    // 1. Waiting for websocket data
                    // 2. Waiting on a flush to complete (backpressure being applied)

                    // Give the receiving task time to complete gracefully with a close message
                    var resultTask = await Task.WhenAny(receiving, Task.Delay(_closeTimeout));

                    if (resultTask != receiving)
                    {
                        // _logger.CloseTimedOut();

                        // Cancel any pending flush so that we can quit
                        _application.Output.CancelPendingFlush();

                        // We didn't complete so abort the websocket
                        socket.Abort();
                    }
                }
            }
        }

        private async Task StartReceiving(WebSocket socket)
        {
            try
            {
                while (true)
                {
                    var memory = _application.Output.GetMemory();

#if NETCOREAPP2_1
                    var receiveResult = await socket.ReceiveAsync(memory, CancellationToken.None);
#else
                    var isArray = memory.TryGetArray(out var arraySegment);
                    Debug.Assert(isArray);

                    // Exceptions are handled above where the send and receive tasks are being run.
                    var receiveResult = await socket.ReceiveAsync(arraySegment, CancellationToken.None);
#endif
                    if (receiveResult.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.WebSocketClosed(_webSocket.CloseStatus);

                        if (_webSocket.CloseStatus != WebSocketCloseStatus.NormalClosure)
                        {
                            throw new InvalidOperationException($"Websocket closed with error: {_webSocket.CloseStatus}.");
                        }

                        return;
                    }

                    _logger.MessageReceived(receiveResult.MessageType, receiveResult.Count, receiveResult.EndOfMessage);

                    _application.Output.Advance(receiveResult.Count);

                    if (receiveResult.EndOfMessage)
                    {
                        var flushResult = await _application.Output.FlushAsync();

                        // We cancelled in the middle of applying back pressure
                        // or if the consumer is done
                        if (flushResult.IsCancelled || flushResult.IsCompleted)
                        {
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _application.Output.Complete(ex);

                // We re-throw here so we can communicate that there was an error when sending
                // the close frame
                throw;
            }
            finally
            {
                // We're done writing
                _application.Output.Complete();
            }

            _logger.ReceiveStopped();
        }

        private async Task StartSending(WebSocket ws)
        {
            var webSocketMessageType =
                Mode == TransferMode.Binary
                    ? WebSocketMessageType.Binary
                    : WebSocketMessageType.Text;
            try
            {
                while (true)
                {
                    var result = await _application.Input.ReadAsync();
                    var buffer = result.Buffer;

                    // Get a frame from the application

                    try
                    {
                        if (result.IsCancelled)
                        {
                            return;
                        }

                        if (!buffer.IsEmpty)
                        {
                            try
                            {
                                _logger.ReceivedFromApp(buffer.Length);

                                if (WebSocketCanSend(ws))
                                {
                                    await ws.SendAsync(buffer, webSocketMessageType);
                                }
                            }
                            catch (WebSocketException socketException) when (!WebSocketCanSend(ws))
                            {
                                // this can happen when we send the CloseFrame to the client and try to write afterwards
                                _logger.ErrorSendingMessage(socketException);
                                break;
                            }
                            catch (Exception ex)
                            {
                                _logger.ErrorSendingMessage(ex);
                                break;
                            }
                        }
                        else if (result.IsCompleted)
                        {
                            break;
                        }
                    }
                    finally
                    {
                        _application.Input.AdvanceTo(buffer.End);
                    }
                }
            }
            finally
            {
                _application.Input.Complete();
            }

            _logger.SendStopped();
        }

        private static bool WebSocketCanSend(WebSocket ws)
        {
            return !(ws.State == WebSocketState.Aborted ||
                   ws.State == WebSocketState.Closed ||
                   ws.State == WebSocketState.CloseSent);
        }

        private async Task Connect(Uri url)
        {
            var uriBuilder = new UriBuilder(url);
            if (url.Scheme == "http")
            {
                uriBuilder.Scheme = "ws";
            }
            else if (url.Scheme == "https")
            {
                uriBuilder.Scheme = "wss";
            }

            await _webSocket.ConnectAsync(uriBuilder.Uri, CancellationToken.None);
        }

        public async Task StopAsync()
        {
            _logger.TransportStopping();

            // Cancel any pending reads from the application, this should start the entire shutdown process
            _application.Input.CancelPendingRead();

            try
            {
                await Running;
            }
            catch
            {
                // exceptions have been handled in the Running task continuation by closing the channel with the exception
            }
        }
    }
}
