pub(crate) mod websocket {
    use std::ffi::{c_int, c_void, CStr, CString};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use futures::{Sink, Stream, StreamExt};
    use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};

    use super::emscripten_bindings::websocket::*;
    use super::{EmError, EmscriptenResult};
    use message::CloseFrame;

    pub(crate) use error::Error;
    pub(crate) use message::Message;

    pub struct WebSocketStream {
        socket_handle: i32,
        ready_state: ReadyState,
        pub queue: UnboundedReceiver<InternalQueue>,
    }

    pub fn connect_with_protocols(url: &str, protocols: &[&str]) -> Result<WebSocketStream, Error> {
        let (queue_tx, queue_rx) = futures::channel::mpsc::unbounded::<InternalQueue>();

        let url_cstr = CString::new(url).unwrap();
        let protocols_cstr = if !protocols.is_empty() {
            let joined = protocols.join(",");
            Some(CString::new(joined).unwrap())
        } else {
            None
        };

        let mut create_attr = EmscriptenWebSocketCreateAttributes {
            url: url_cstr.as_ptr(),
            protocols: protocols_cstr.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            createOnMainThread: true,
        };

        // If the return value of this function is > 0, the function has succeeded and the return
        // value represents a handle to the WebSocket object.
        // If the return value of this function is < 0, then the function has failed,
        // and the return value can be interpreted as a EMSCRIPTEN_RESULT code
        // representing the cause of the failure. If the function returns 0, then the call has
        // failed with an unknown reason (build with -sWEBSOCKET_DEBUG for more information)
        let socket_handle: i32 = unsafe { emscripten_websocket_new(&mut create_attr) };
        if socket_handle > 0 {
            set_callback_hooks(socket_handle, queue_tx);
        } else {
            return Err(EmError::try_from(socket_handle).unwrap().into());
        }

        Ok(WebSocketStream {
            socket_handle,
            ready_state: ReadyState::Connecting,
            queue: queue_rx,
        })
    }

    impl WebSocketStream {
        pub fn ready_state(&self) -> ReadyState {
            self.ready_state
        }

        pub fn send_binary(&self, bytes: Bytes) -> EmscriptenResult {
            let data: &[u8] = bytes.as_ref();
            unsafe {
                emscripten_websocket_send_binary(self.socket_handle, data.as_ptr() as *mut c_void, data.len() as u32)
            }
            .into()
        }

        pub fn send_text(&self, message: String) -> EmscriptenResult {
            let text = CString::new(message.replace("\0", "")).unwrap();
            unsafe { emscripten_websocket_send_utf8_text(self.socket_handle, text.as_ptr()) }.into()
        }

        pub fn close_with_reason(&mut self, code: u16, reason: String) -> EmscriptenResult {
            self.ready_state = ReadyState::Closing;
            let reason_cstr = CString::new(reason).unwrap();
            unsafe { emscripten_websocket_close(self.socket_handle, code, reason_cstr.as_ptr()) }.into()
        }

        pub fn close(&mut self) -> EmscriptenResult {
            self.ready_state = ReadyState::Closing;
            let reason = CString::new("").unwrap();
            unsafe { emscripten_websocket_close(self.socket_handle, 1000, reason.as_ptr()) }.into()
        }
    }

    impl Stream for WebSocketStream {
        type Item = error::Result<Message>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let waker = cx.waker().clone();
            match self.queue.poll_next_unpin(cx) {
                Poll::Ready(Some(internal_q)) => match internal_q {
                    InternalQueue::StateChange(ready_state) => {
                        self.ready_state = ready_state;
                        waker.wake();
                        Poll::Pending
                    }
                    InternalQueue::Message(msg) => Poll::Ready(Some(msg)),
                },
                Poll::Ready(None) => Poll::Ready(None), // channel closed
                Poll::Pending => match self.ready_state() {
                    ReadyState::Closed => Poll::Ready(None),
                    _ => Poll::Pending,
                },
            }
        }
    }

    impl Sink<Message> for WebSocketStream {
        type Error = Error;

        fn poll_ready(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            match self.ready_state() {
                ReadyState::Open => Ok(()).into(),
                ReadyState::Connecting => {
                    if let Ok(Some(InternalQueue::StateChange(s))) = self.queue.try_next() {
                        self.ready_state = s;
                        if s == ReadyState::Open {
                            return Ok(()).into();
                        }
                    }
                    Poll::Pending
                }
                _ => Err(Error::ConnectionClosed).into(),
            }
        }

        fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            match self.ready_state() {
                ReadyState::Open => match item {
                    Message::Text(text) => self.send_text(text).into(),
                    Message::Binary(bytes) => self.send_binary(bytes).into(),
                    Message::Close(frame) => match frame {
                        Some(frame) => self.close_with_reason(frame.code(), frame.reason()).into(),
                        None => self.close().into(),
                    },
                },
                _ => Err(Error::ConnectionClosed),
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Ok(()).into()
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Ok(()).into()
        }
    }

    #[repr(u16)]
    #[derive(Clone, Debug, Copy, PartialEq, Eq)]
    pub enum ReadyState {
        Connecting = 0,
        Open = 1,
        Closing = 2,
        Closed = 3,
    }

    #[derive(Debug)]
    pub enum InternalQueue {
        StateChange(ReadyState),
        Message(Result<Message, Error>),
    }

    struct CloseCallback {
        closure: Box<dyn Fn(CloseFrame) + Send + 'static>,
    }
    struct OpenCallback {
        closure: Box<dyn Fn(ReadyState) + Send + 'static>,
    }
    struct MessageCallback {
        closure: Box<dyn Fn(Message) + Send + 'static>,
    }

    fn set_callback_hooks(handle_id: i32, queue_tx: UnboundedSender<InternalQueue>) {
        let message_cb = {
            let queue_c = queue_tx.clone();
            let closure = Box::new(move |message| {
                let _ = queue_c.unbounded_send(InternalQueue::Message(Ok(message)));
            });
            Box::new(MessageCallback { closure })
        };

        let open_cb = {
            let queue_c = queue_tx.clone();
            let closure = Box::new(move |state_change| {
                let _ = queue_c.unbounded_send(InternalQueue::StateChange(state_change));
            });
            Box::new(OpenCallback { closure })
        };

        let close_cb = {
            let queue_c = queue_tx.clone();
            let closure = Box::new(move |close_frame| {
                let _ = queue_c.unbounded_send(InternalQueue::StateChange(ReadyState::Closed));
                let _ = queue_c.unbounded_send(InternalQueue::Message(Ok(Message::Close(Some(close_frame)))));
            });
            Box::new(CloseCallback { closure })
        };

        // reference:
        // #define EM_CALLBACK_THREAD_CONTEXT_MAIN_RUNTIME_THREAD ((pthread_t)0x1)
        // #define EM_CALLBACK_THREAD_CONTEXT_CALLING_THREAD ((pthread_t)0x2)
        // TODO: Add OnErrorCallback
        unsafe {
            emscripten_websocket_set_onclose_callback_on_thread(
                handle_id,
                Box::into_raw(close_cb) as *mut c_void,
                Some(on_close_ws),
                0x2 as *mut __pthread,
            );
            // emscripten_websocket_set_onerror_callback_on_thread(
            //     handle_id,
            //     Box::into_raw(error_cb) as *mut c_void,
            //     Some(on_error_ws),
            //     0x2 as *mut __pthread,
            // );
            emscripten_websocket_set_onmessage_callback_on_thread(
                handle_id,
                Box::into_raw(message_cb) as *mut c_void,
                Some(on_message_ws),
                0x2 as *mut __pthread,
            );
            emscripten_websocket_set_onopen_callback_on_thread(
                handle_id,
                Box::into_raw(open_cb) as *mut c_void,
                Some(on_open_ws),
                0x2 as *mut __pthread,
            );
        }
    }

    unsafe extern "C" fn on_message_ws(
        _event: c_int,
        message_ev_ptr: *const EmscriptenWebSocketMessageEvent,
        callback_ptr: *mut c_void,
    ) -> bool {
        let msg_event = &*message_ev_ptr;
        let msg_bytes = if !msg_event.data.is_null() {
            unsafe { std::slice::from_raw_parts(msg_event.data as *const u8, msg_event.numBytes as usize) }.to_vec()
        } else {
            Vec::new()
        };
        let message = if msg_event.isText {
            Message::text(String::from_utf8(msg_bytes).unwrap())
        } else {
            Message::binary(Bytes::from(msg_bytes))
        };

        let message_cb = &mut *(callback_ptr as *mut MessageCallback);
        (message_cb.closure)(message);
        true
    }

    unsafe extern "C" fn on_close_ws(
        _event: c_int,
        close_ev_ptr: *const EmscriptenWebSocketCloseEvent,
        callback_ptr: *mut c_void,
    ) -> bool {
        let close_ev = if !close_ev_ptr.is_null() {
            CloseFrame::new(
                (*close_ev_ptr).wasClean,
                (*close_ev_ptr).code,
                CStr::from_ptr((*close_ev_ptr).reason.as_ptr())
                    .to_string_lossy()
                    .into_owned(),
            )
        } else {
            CloseFrame::new(
                false,
                1006, // 1006: Abnormal Closure / Connection lost and no close frame received
                "".into(),
            )
        };

        let close_cb = &mut *(callback_ptr as *mut CloseCallback);
        (close_cb.closure)(close_ev);
        true
    }

    unsafe extern "C" fn on_open_ws(
        _event: c_int,
        _open_ev_ptr: *const EmscriptenWebSocketOpenEvent,
        callback_ptr: *mut c_void,
    ) -> bool {
        let open_cb = &mut *(callback_ptr as *mut OpenCallback);
        (open_cb.closure)(ReadyState::Open);
        true
    }

    unsafe extern "C" fn _on_error_ws(
        _event: c_int,
        _error_ev_ptr: *const EmscriptenWebSocketErrorEvent,
        _callback_ptr: *mut c_void,
    ) -> bool {
        // TODO: Handle error event
        false
    }

    impl Drop for WebSocketStream {
        fn drop(&mut self) {
            let handle_id = self.socket_handle;
            unsafe {
                emscripten_websocket_set_onclose_callback_on_thread(
                    handle_id,
                    std::ptr::null_mut::<c_void>(),
                    None,
                    0x2 as *mut __pthread,
                );
                // emscripten_websocket_set_onerror_callback_on_thread(
                //     handle_id,
                //     std::ptr::null_mut::<c_void>(),
                //     None,
                //     0x2 as *mut __pthread,
                // );
                emscripten_websocket_set_onmessage_callback_on_thread(
                    handle_id,
                    std::ptr::null_mut::<c_void>(),
                    None,
                    0x2 as *mut __pthread,
                );
                emscripten_websocket_set_onopen_callback_on_thread(
                    handle_id,
                    std::ptr::null_mut::<c_void>(),
                    None,
                    0x2 as *mut __pthread,
                );

                emscripten_websocket_delete(handle_id);
            }
        }
    }

    pub mod message {
        use bytes::Bytes;

        #[derive(Debug)]
        pub enum Message {
            Text(String),
            Binary(Bytes),
            Close(Option<CloseFrame>),
        }

        #[derive(Debug)]
        pub struct CloseFrame {
            clean: bool,
            code: u16,
            reason: String,
        }

        impl CloseFrame {
            pub fn new(clean: bool, code: u16, reason: String) -> Self {
                Self { clean, code, reason }
            }

            pub fn was_clean(&self) -> bool {
                self.clean
            }

            pub fn code(&self) -> u16 {
                self.code
            }

            pub fn reason(&self) -> String {
                self.reason.clone()
            }
        }

        impl Message {
            /// Create a new text WebSocket message from a stringable.
            pub fn text<S>(string: S) -> Message
            where
                S: Into<String>,
            {
                Message::Text(string.into())
            }

            /// Create a new binary WebSocket message by converting to Vec<u8>.
            pub fn binary<B>(bin: B) -> Message
            where
                B: Into<Bytes>,
            {
                Message::Binary(bin.into())
            }

            /// Indicates whether a message is a text message.
            pub fn is_text(&self) -> bool {
                matches!(*self, Message::Text(_))
            }

            /// Indicates whether a message is a binary message.
            pub fn is_binary(&self) -> bool {
                matches!(*self, Message::Binary(_))
            }

            /// Indicates whether a message ia s close message.
            pub fn is_close(&self) -> bool {
                matches!(*self, Message::Close(_))
            }

            /// Get the length of the WebSocket message.
            pub fn len(&self) -> usize {
                match *self {
                    Message::Text(ref string) => string.len(),
                    Message::Binary(ref data) => data.len(),
                    Message::Close(ref data) => data.as_ref().map(|d| d.reason.len()).unwrap_or(0),
                }
            }

            /// Returns true if the WebSocket message has no content.
            /// For example, if the other side of the connection sent an empty string.
            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }

            /// Consume the WebSocket and return it as binary data.
            pub fn into_data(self) -> Bytes {
                match self {
                    Message::Text(string) => string.into(),
                    Message::Binary(data) => data,
                    Message::Close(None) => Bytes::new(),
                    Message::Close(Some(frame)) => frame.reason.into(),
                }
            }
        }

        impl From<String> for Message {
            fn from(string: String) -> Self {
                Message::text(string)
            }
        }

        impl<'s> From<&'s str> for Message {
            fn from(string: &'s str) -> Self {
                Message::text(string)
            }
        }

        impl<'b> From<&'b [u8]> for Message {
            fn from(data: &'b [u8]) -> Self {
                Message::binary(Bytes::copy_from_slice(data))
            }
        }

        impl From<Vec<u8>> for Message {
            fn from(data: Vec<u8>) -> Self {
                Message::binary(data)
            }
        }

        impl From<Message> for Bytes {
            fn from(message: Message) -> Self {
                message.into_data()
            }
        }
    }

    // HACK: Borrowing Tungstenite error for now
    // TODO: Implement Error
    mod error {
        #![allow(clippy::enum_variant_names)]

        use super::{EmError, EmscriptenResult};

        use super::Message;

        use std::{io, result, str, string};

        use thiserror::Error;

        /// Result type of all Tungstenite library calls.
        pub type Result<T, E = Error> = result::Result<T, E>;

        /// Possible WebSocket errors.
        #[derive(Error, Debug)]
        pub enum Error {
            /// WebSocket connection closed normally. This informs you of the close.
            /// It's not an error as such and nothing wrong happened.
            ///
            /// This is returned as soon as the close handshake is finished (we have both sent and
            /// received a close frame) on the server end and as soon as the server has closed the
            /// underlying connection if this endpoint is a client.
            ///
            /// Thus when you receive this, it is safe to drop the underlying connection.
            ///
            /// Receiving this error means that the WebSocket object is not usable anymore and the
            /// only meaningful action with it is dropping it.
            #[error("Connection closed normally")]
            ConnectionClosed,
            /// Trying to work with already closed connection.
            ///
            /// Trying to read or write after receiving `ConnectionClosed` causes this.
            ///
            /// As opposed to `ConnectionClosed`, this indicates your code tries to operate on the
            /// connection when it really shouldn't anymore, so this really indicates a programmer
            /// error on your part.
            #[error("Trying to work with closed connection")]
            AlreadyClosed,
            /// Input-output error. Apart from WouldBlock, these are generally errors with the
            /// underlying connection and you should probably consider them fatal.
            #[error("IO error: {0}")]
            Io(#[from] io::Error),
            /// TLS error.
            ///
            /// Note that this error variant is enabled unconditionally even if no TLS feature is enabled,
            /// to provide a feature-agnostic API surface.
            // #[error("TLS error: {0}")]
            // Tls(#[from] TlsError),
            /// - When reading: buffer capacity exhausted.
            /// - When writing: your message is bigger than the configured max message size
            ///   (64MB by default).
            #[error("Space limit exceeded: {0}")]
            Capacity(#[from] CapacityError),
            /// Protocol violation.
            #[error("WebSocket protocol error: {0}")]
            Protocol(#[from] ProtocolError),
            /// Message write buffer is full.
            #[error("Write buffer is full")]
            WriteBufferFull(Message),
            /// UTF coding error.
            #[error("UTF-8 encoding error: {0}")]
            Utf8(String),
            /// Attack attempt detected.
            #[error("Attack attempt detected")]
            AttackAttempt,
            /// Invalid URL.
            #[error("URL error: {0}")]
            Url(#[from] UrlError),
            /// FIXME: TEMPORARY VARIANT
            #[error("EmscriptenResult Error")]
            Emscripten(#[from] EmError),
        }

        impl From<EmscriptenResult> for Result<()> {
            fn from(value: EmscriptenResult) -> Self {
                match value {
                    EmscriptenResult::Success | EmscriptenResult::Deferred => Ok(()),
                    EmscriptenResult::Error(e) => Err(e.into()),
                }
            }
        }

        impl From<str::Utf8Error> for Error {
            fn from(err: str::Utf8Error) -> Self {
                Error::Utf8(err.to_string())
            }
        }

        impl From<string::FromUtf8Error> for Error {
            fn from(err: string::FromUtf8Error) -> Self {
                Error::Utf8(err.to_string())
            }
        }
        /// Indicates the specific type/cause of a capacity error.
        #[derive(Error, Debug, PartialEq, Eq, Clone, Copy)]
        pub enum CapacityError {
            /// Too many headers provided (see [`httparse::Error::TooManyHeaders`]).
            #[error("Too many headers")]
            TooManyHeaders,
            /// Received header is too long.
            /// Message is bigger than the maximum allowed size.
            #[error("Message too long: {size} > {max_size}")]
            MessageTooLong {
                /// The size of the message.
                size: usize,
                /// The maximum allowed message size.
                max_size: usize,
            },
        }

        /// Indicates the specific type/cause of a subprotocol header error.
        #[derive(Error, Clone, PartialEq, Eq, Debug, Copy)]
        pub enum SubProtocolError {
            /// The server sent a subprotocol to a client handshake request but none was requested
            #[error("Server sent a subprotocol but none was requested")]
            ServerSentSubProtocolNoneRequested,

            /// The server sent an invalid subprotocol to a client handhshake request
            #[error("Server sent an invalid subprotocol")]
            InvalidSubProtocol,

            /// The server sent no subprotocol to a client handshake request that requested one or more
            /// subprotocols
            #[error("Server sent no subprotocol")]
            NoSubProtocol,
        }

        /// Indicates the specific type/cause of a protocol error.
        #[allow(missing_copy_implementations)]
        #[derive(Error, Debug, PartialEq, Eq, Clone)]
        pub enum ProtocolError {
            /// Use of the wrong HTTP method (the WebSocket protocol requires the GET method be used).
            #[error("Unsupported HTTP method used - only GET is allowed")]
            WrongHttpMethod,
            /// Wrong HTTP version used (the WebSocket protocol requires version 1.1 or higher).
            #[error("HTTP version must be 1.1 or higher")]
            WrongHttpVersion,
            /// Missing `Connection: upgrade` HTTP header.
            #[error("No \"Connection: upgrade\" header")]
            MissingConnectionUpgradeHeader,
            /// Missing `Upgrade: websocket` HTTP header.
            #[error("No \"Upgrade: websocket\" header")]
            MissingUpgradeWebSocketHeader,
            /// Missing `Sec-WebSocket-Version: 13` HTTP header.
            #[error("No \"Sec-WebSocket-Version: 13\" header")]
            MissingSecWebSocketVersionHeader,
            /// Missing `Sec-WebSocket-Key` HTTP header.
            #[error("No \"Sec-WebSocket-Key\" header")]
            MissingSecWebSocketKey,
            /// The `Sec-WebSocket-Accept` header is either not present or does not specify the correct key value.
            #[error("Key mismatch in \"Sec-WebSocket-Accept\" header")]
            SecWebSocketAcceptKeyMismatch,
            /// The `Sec-WebSocket-Protocol` header was invalid
            #[error("SubProtocol error: {0}")]
            SecWebSocketSubProtocolError(SubProtocolError),
            /// Garbage data encountered after client request.
            #[error("Junk after client request")]
            JunkAfterRequest,
            /// Custom responses must be unsuccessful.
            #[error("Custom response must not be successful")]
            CustomResponseSuccessful,
            /// Invalid header is passed. Or the header is missing in the request. Or not present at all. Check the request that you pass.
            // #[error("Missing, duplicated or incorrect header {0}")]
            // #[cfg(feature = "handshake")]
            // InvalidHeader(HeaderName),
            /// No more data while still performing handshake.
            #[error("Handshake not finished")]
            HandshakeIncomplete,
            /// Wrapper around a [`httparse::Error`] value.
            // #[error("httparse error: {0}")]
            // #[cfg(feature = "handshake")]
            // HttparseError(#[from] httparse::Error),
            /// Not allowed to send after having sent a closing frame.
            #[error("Sending after closing is not allowed")]
            SendAfterClosing,
            /// Remote sent data after sending a closing frame.
            #[error("Remote sent after having closed")]
            ReceivedAfterClosing,
            /// Reserved bits in frame header are non-zero.
            #[error("Reserved bits are non-zero")]
            NonZeroReservedBits,
            /// The server must close the connection when an unmasked frame is received.
            #[error("Received an unmasked frame from client")]
            UnmaskedFrameFromClient,
            /// The client must close the connection when a masked frame is received.
            #[error("Received a masked frame from server")]
            MaskedFrameFromServer,
            /// Control frames must not be fragmented.
            #[error("Fragmented control frame")]
            FragmentedControlFrame,
            /// Control frames must have a payload of 125 bytes or less.
            #[error("Control frame too big (payload must be 125 bytes or less)")]
            ControlFrameTooBig,
            /// Type of control frame not recognised.
            #[error("Unknown control frame type: {0}")]
            UnknownControlFrameType(u8),
            /// Type of data frame not recognised.
            #[error("Unknown data frame type: {0}")]
            UnknownDataFrameType(u8),
            /// Received a continue frame despite there being nothing to continue.
            #[error("Continue frame but nothing to continue")]
            UnexpectedContinueFrame,
            /// Received data while waiting for more fragments.
            #[error("While waiting for more fragments received: {0}")]
            ExpectedFragment(Data),
            /// Connection closed without performing the closing handshake.
            #[error("Connection reset without closing handshake")]
            ResetWithoutClosingHandshake,
            /// Encountered an invalid opcode.
            #[error("Encountered invalid opcode: {0}")]
            InvalidOpcode(u8),
            /// The payload for the closing frame is invalid.
            #[error("Invalid close sequence")]
            InvalidCloseSequence,
        }

        /// Indicates the specific type/cause of URL error.
        #[derive(Error, Debug, PartialEq, Eq)]
        pub enum UrlError {
            /// TLS is used despite not being compiled with the TLS feature enabled.
            #[error("TLS support not compiled in")]
            TlsFeatureNotEnabled,
            /// The URL does not include a host name.
            #[error("No host name in the URL")]
            NoHostName,
            /// Failed to connect with this URL.
            #[error("Unable to connect to {0}")]
            UnableToConnect(String),
            /// Unsupported URL scheme used (only `ws://` or `wss://` may be used).
            #[error("URL scheme not supported")]
            UnsupportedUrlScheme,
            /// The URL host name, though included, is empty.
            #[error("URL contains empty host name")]
            EmptyHostName,
            /// The URL does not include a path/query.
            #[error("No path/query in URL")]
            NoPathOrQuery,
        }

        /*
        /// TLS errors.
        ///
        /// Note that even if you enable only the rustls-based TLS support, the error at runtime could still
        /// be `Native`, as another crate in the dependency graph may enable native TLS support.
        #[allow(missing_copy_implementations)]
        #[derive(Error, Debug)]
        #[non_exhaustive]
        pub enum TlsError {
            /// Native TLS error.
            #[cfg(feature = "native-tls")]
            #[error("native-tls error: {0}")]
            Native(#[from] native_tls_crate::Error),
            /// Rustls error.
            #[cfg(feature = "__rustls-tls")]
            #[error("rustls error: {0}")]
            Rustls(#[from] rustls::Error),
            /// DNS name resolution error.
            #[cfg(feature = "__rustls-tls")]
            #[error("Invalid DNS name")]
            InvalidDnsName,
            /// Unknown
            #[error("An unknown error from the underlying interface")]
            Unknown,
        }
        */

        /// Data opcodes as in RFC 6455
        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        pub enum Data {
            /// 0x0 denotes a continuation frame
            Continue,
            /// 0x1 denotes a text frame
            Text,
            /// 0x2 denotes a binary frame
            Binary,
            /// 0x3-7 are reserved for further non-control frames
            Reserved(u8),
        }

        impl std::fmt::Display for Data {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                match *self {
                    Data::Continue => write!(f, "CONTINUE"),
                    Data::Text => write!(f, "TEXT"),
                    Data::Binary => write!(f, "BINARY"),
                    Data::Reserved(x) => write!(f, "RESERVED_DATA_{}", x),
                }
            }
        }
    }
}

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmscriptenResult {
    /// Operation succeeded (return code 0)
    Success,
    /// Operation deferred for later completion (return code 1)
    Deferred,
    /// Error cases (negative return codes)
    Error(EmError),
}

#[repr(i32)]
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmError {
    #[error("Operation not supported")]
    NotSupported = -1,
    #[error("Operation failed without deferred result")]
    FailedNotDeferred = -2,
    #[error("Invalid target")]
    InvalidTarget = -3,
    #[error("Unknown target")]
    UnknownTarget = -4,
    #[error("Invalid parameter")]
    InvalidParam = -5,
    #[error("Generic failure")]
    Failed = -6,
    #[error("No data available")]
    NoData = -7,
    #[error("Operation timed out")]
    TimedOut = -8,
    #[error("Unknown error code: {0}")]
    UnknownCode(i32) = i32::MIN,
}

impl EmscriptenResult {
    /// Returns true if result is considered successful
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Success | Self::Deferred)
    }

    /// Returns true if result is an error
    pub fn is_err(&self) -> bool {
        matches!(self, Self::Error(_))
    }
}

impl From<i32> for EmscriptenResult {
    fn from(code: i32) -> Self {
        match code {
            0 => Self::Success,
            1 => Self::Deferred,
            n if n < 0 => Self::Error(EmError::try_from(n).unwrap_or(EmError::UnknownCode(n))),
            unknown => Self::Error(EmError::UnknownCode(unknown)),
        }
    }
}

impl TryFrom<i32> for EmError {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            -1 => Ok(Self::NotSupported),
            -2 => Ok(Self::FailedNotDeferred),
            -3 => Ok(Self::InvalidTarget),
            -4 => Ok(Self::UnknownTarget),
            -5 => Ok(Self::InvalidParam),
            -6 => Ok(Self::Failed),
            -7 => Ok(Self::NoData),
            -8 => Ok(Self::TimedOut),
            _ => Err(()),
        }
    }
}

impl From<EmscriptenResult> for Result<(), EmError> {
    fn from(result: EmscriptenResult) -> Self {
        match result {
            EmscriptenResult::Success | EmscriptenResult::Deferred => Ok(()),
            EmscriptenResult::Error(e) => Err(e),
        }
    }
}

pub(crate) mod fetch {
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::ffi::{c_char, CStr, CString};

    use super::emscripten_bindings::fetch::*;

    pub struct FetchResponse {
        pub status: u16,
        pub status_text: String,
        pub bytes: Vec<u8>,
    }

    impl FetchResponse {
        pub fn text(&self) -> Result<Cow<'_, str>, std::str::Utf8Error> {
            Ok(String::from_utf8_lossy(&self.bytes))
        }

        unsafe fn from_raw(fetch_response: *mut emscripten_fetch_t) -> Self {
            let status = (*fetch_response).status;
            let num_bytes = (*fetch_response).numBytes;
            let data_ptr = (*fetch_response).data;
            let status_text = CStr::from_ptr((*fetch_response).statusText.as_ptr())
                .to_string_lossy()
                .into_owned();
            let mut bytes = Vec::new();
            if !data_ptr.is_null() && num_bytes > 0 {
                bytes.extend_from_slice(std::slice::from_raw_parts(data_ptr as *const u8, num_bytes as usize));
            }

            FetchResponse {
                status,
                status_text,
                bytes,
            }
        }
    }

    #[derive(Debug, Default)]
    pub struct Request {
        method: String,
        url: String,
        headers: HashMap<String, String>,
        body: Option<Vec<u8>>,
        timeout: u32,
        with_credentials: bool,
    }

    impl Request {
        pub fn new(url: impl ToString) -> Self {
            Request {
                url: url.to_string(),
                method: "GET".into(),
                ..Default::default()
            }
        }

        pub fn method<T>(mut self, method: T) -> Self
        where
            T: AsRef<str>,
        {
            self.method = method.as_ref().to_uppercase();
            self
        }

        pub fn header<T>(mut self, name: T, value: T) -> Self
        where
            T: AsRef<str>,
        {
            self.headers.insert(name.as_ref().into(), value.as_ref().into());
            self
        }

        pub fn body(mut self, body: Vec<u8>) -> Self {
            self.body = Some(body);
            self
        }

        pub fn timeout(mut self, timeout_ms: u32) -> Self {
            self.timeout = timeout_ms;
            self
        }

        pub fn with_credentials(mut self, with_credentials: bool) -> Self {
            self.with_credentials = with_credentials;
            self
        }

        pub fn send_sync(self) -> Result<FetchResponse, String> {
            let url_cstr = CString::new(self.url.as_str()).unwrap();
            let method_cstr = CString::new(self.method.as_str()).unwrap();

            let mut request_headers_ptrs: Vec<*const c_char> = Vec::new();
            for (name, value) in &self.headers {
                let header = CString::new(format!("{}: {}", name, value)).unwrap();
                request_headers_ptrs.push(header.as_ptr());
            }
            request_headers_ptrs.push(std::ptr::null());

            let (request_data_ptr, request_data_size) = if let Some(ref body) = self.body {
                (body.as_ptr() as *const std::os::raw::c_char, body.len())
            } else {
                (std::ptr::null(), 0)
            };

            unsafe {
                let mut attr: emscripten_fetch_attr_t = std::mem::zeroed();
                emscripten_fetch_attr_init(&mut attr);
                attr.attributes =
                    EMSCRIPTEN_FETCH_LOAD_TO_MEMORY | EMSCRIPTEN_FETCH_SYNCHRONOUS | EMSCRIPTEN_FETCH_REPLACE;
                attr.timeoutMSecs = self.timeout;
                attr.withCredentials = self.with_credentials;
                attr.requestHeaders = request_headers_ptrs.as_ptr();
                attr.requestData = request_data_ptr;
                attr.requestDataSize = request_data_size;

                let method_bytes = method_cstr.as_bytes_with_nul();
                let req_method_len = method_bytes.len().min(attr.requestMethod.len());
                std::ptr::copy_nonoverlapping(
                    method_bytes.as_ptr() as *const i8,
                    attr.requestMethod.as_mut_ptr(),
                    req_method_len - 1,
                );
                attr.requestMethod[req_method_len - 1] = 0;

                // synchronously fetch the respsonse
                let em_fetch_response = emscripten_fetch(&mut attr, url_cstr.as_ptr());

                if em_fetch_response.is_null() {
                    return Err("Failed to initiate fetch request".into());
                }

                let status = (*em_fetch_response).status;

                let respsonse = if (200..300).contains(&status) {
                    let response = FetchResponse::from_raw(em_fetch_response);
                    Ok(response)
                } else {
                    let status_text = CStr::from_ptr((*em_fetch_response).statusText.as_ptr())
                        .to_string_lossy()
                        .into_owned();
                    Err(format!("HTTP error: {} - {}", status, status_text))
                };
                emscripten_fetch_close(em_fetch_response);
                respsonse
            }
        }
    }
}

mod emscripten_bindings {
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]

    // Bindings generated using rust-bindgen `0.66.1`` from emscripten `3.1.74` and unified here

    pub(super) mod websocket {
        use std::ffi::{c_char, c_int, c_ushort, c_void};

        #[repr(C)]
        #[derive(Debug, Copy, Clone)]
        pub struct EmscriptenWebSocketCreateAttributes {
            pub url: *const c_char,
            pub protocols: *const c_char,
            pub createOnMainThread: bool,
        }

        #[repr(C)]
        #[derive(Debug, Copy, Clone)]
        pub struct EmscriptenWebSocketCloseEvent {
            pub socket: c_int,
            pub wasClean: bool,
            pub code: c_ushort,
            pub reason: [c_char; 512usize],
        }

        #[repr(C)]
        #[derive(Debug, Copy, Clone)]
        pub struct EmscriptenWebSocketErrorEvent {
            pub socket: c_int,
        }

        #[repr(C)]
        #[derive(Debug, Copy, Clone)]
        pub struct EmscriptenWebSocketMessageEvent {
            pub socket: c_int,
            pub data: *mut u8,
            pub numBytes: u32,
            pub isText: bool,
        }

        #[repr(C)]
        #[derive(Debug, Copy, Clone)]
        pub struct EmscriptenWebSocketOpenEvent {
            pub socket: c_int,
        }

        pub type em_websocket_close_callback_func = Option<
            unsafe extern "C" fn(
                eventType: c_int,
                websocketEvent: *const EmscriptenWebSocketCloseEvent,
                userData: *mut c_void,
            ) -> bool,
        >;

        pub type em_websocket_error_callback_func = Option<
            unsafe extern "C" fn(
                eventType: c_int,
                websocketEvent: *const EmscriptenWebSocketErrorEvent,
                userData: *mut c_void,
            ) -> bool,
        >;

        pub type em_websocket_message_callback_func = Option<
            unsafe extern "C" fn(
                eventType: c_int,
                websocketEvent: *const EmscriptenWebSocketMessageEvent,
                userData: *mut c_void,
            ) -> bool,
        >;

        pub type em_websocket_open_callback_func = Option<
            unsafe extern "C" fn(
                eventType: c_int,
                websocketEvent: *const EmscriptenWebSocketOpenEvent,
                userData: *mut c_void,
            ) -> bool,
        >;

        #[repr(C)]
        #[derive(Debug, Copy, Clone)]
        pub struct __pthread {
            _unused: [u8; 0],
        }
        pub type pthread_t = *mut __pthread;

        extern "C" {
            pub fn emscripten_websocket_close(socket: c_int, code: c_ushort, reason: *const c_char) -> c_int;
            pub fn emscripten_websocket_delete(socket: c_int) -> c_int;
            pub fn emscripten_websocket_get_buffered_amount(socket: c_int, bufferedAmount: *mut usize) -> c_int;
            pub fn emscripten_websocket_get_protocol(
                socket: c_int,
                protocol: *mut c_char,
                protocolLength: c_int,
            ) -> c_int;
            pub fn emscripten_websocket_get_protocol_length(socket: c_int, protocolLength: *mut c_int) -> c_int;
            pub fn emscripten_websocket_get_url(socket: c_int, url: *mut c_char, urlLength: c_int) -> c_int;
            pub fn emscripten_websocket_get_url_length(socket: c_int, urlLength: *mut c_int) -> c_int;
            pub fn emscripten_websocket_is_supported() -> bool;
            pub fn emscripten_websocket_new(createAttributes: *mut EmscriptenWebSocketCreateAttributes) -> c_int;
            pub fn emscripten_websocket_send_binary(socket: c_int, binaryData: *mut c_void, dataLength: u32) -> c_int;
            pub fn emscripten_websocket_send_utf8_text(socket: c_int, textData: *const c_char) -> c_int;
            pub fn emscripten_websocket_set_onclose_callback_on_thread(
                socket: c_int,
                userData: *mut c_void,
                callback: em_websocket_close_callback_func,
                targetThread: pthread_t,
            ) -> c_int;
            pub fn emscripten_websocket_set_onerror_callback_on_thread(
                socket: c_int,
                userData: *mut c_void,
                callback: em_websocket_error_callback_func,
                targetThread: pthread_t,
            ) -> c_int;
            pub fn emscripten_websocket_set_onmessage_callback_on_thread(
                socket: c_int,
                userData: *mut c_void,
                callback: em_websocket_message_callback_func,
                targetThread: pthread_t,
            ) -> c_int;

            pub fn emscripten_websocket_set_onopen_callback_on_thread(
                socket: c_int,
                userData: *mut c_void,
                callback: em_websocket_open_callback_func,
                targetThread: pthread_t,
            ) -> c_int;
        }
    }

    pub(super) mod fetch {
        use std::ffi::{c_char, c_int, c_ushort, c_void};

        pub const EMSCRIPTEN_FETCH_LOAD_TO_MEMORY: u32 = 1;
        pub const EMSCRIPTEN_FETCH_STREAM_DATA: u32 = 2;
        pub const EMSCRIPTEN_FETCH_PERSIST_FILE: u32 = 4;
        pub const EMSCRIPTEN_FETCH_APPEND: u32 = 8;
        pub const EMSCRIPTEN_FETCH_REPLACE: u32 = 16;
        pub const EMSCRIPTEN_FETCH_NO_DOWNLOAD: u32 = 32;
        pub const EMSCRIPTEN_FETCH_SYNCHRONOUS: u32 = 64;
        pub const EMSCRIPTEN_FETCH_WAITABLE: u32 = 128;

        #[repr(C)]
        #[derive(Debug, Copy, Clone)]
        pub struct emscripten_fetch_attr_t {
            pub requestMethod: [c_char; 32usize],
            pub userData: *mut c_void,
            pub onsuccess: Option<unsafe extern "C" fn(fetch: *mut emscripten_fetch_t)>,
            pub onerror: Option<unsafe extern "C" fn(fetch: *mut emscripten_fetch_t)>,
            pub onprogress: Option<unsafe extern "C" fn(fetch: *mut emscripten_fetch_t)>,
            pub onreadystatechange: Option<unsafe extern "C" fn(fetch: *mut emscripten_fetch_t)>,
            pub attributes: u32,
            pub timeoutMSecs: u32,
            pub withCredentials: bool,
            pub destinationPath: *const c_char,
            pub userName: *const c_char,
            pub password: *const c_char,
            pub requestHeaders: *const *const c_char,
            pub overriddenMimeType: *const c_char,
            pub requestData: *const c_char,
            pub requestDataSize: usize,
        }

        #[repr(C)]
        #[derive(Debug, Copy, Clone)]
        pub struct emscripten_fetch_t {
            pub id: u32,
            pub userData: *mut c_void,
            pub url: *const c_char,
            pub data: *const c_char,
            pub numBytes: u64,
            pub dataOffset: u64,
            pub totalBytes: u64,
            pub readyState: c_ushort,
            pub status: c_ushort,
            pub statusText: [c_char; 64usize],
            pub __attributes: emscripten_fetch_attr_t,
        }

        extern "C" {
            pub fn emscripten_fetch(
                fetch_attr: *mut emscripten_fetch_attr_t,
                url: *const c_char,
            ) -> *mut emscripten_fetch_t;

            pub fn emscripten_fetch_attr_init(fetch_attr: *mut emscripten_fetch_attr_t);
            pub fn emscripten_fetch_close(fetch: *mut emscripten_fetch_t) -> c_int;

        }
    }
}
