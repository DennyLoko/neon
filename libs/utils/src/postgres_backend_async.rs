//! Server-side asynchronous Postgres connection, as limited as we need.
//! To use, create PostgresBackend and run() it, passing the Handler
//! implementation determining how to process the queries. Currently its API
//! is rather narrow, but we can extend it once required.

use crate::postgres_backend::AuthType;
use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use pq_proto::{BeMessage, FeMessage, FeStartupPacket};
use rand::Rng;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tracing::{debug, error, trace};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_rustls::TlsAcceptor;

#[async_trait::async_trait]
pub trait Handler {
    /// Handle single query.
    /// postgres_backend will issue ReadyForQuery after calling this (this
    /// might be not what we want after CopyData streaming, but currently we don't
    /// care).
    async fn process_query(&mut self, pgb: &mut PostgresBackend, query_string: &str) -> Result<()>;

    /// Called on startup packet receival, allows to process params.
    ///
    /// If Ok(false) is returned postgres_backend will skip auth -- that is needed for new users
    /// creation is the proxy code. That is quite hacky and ad-hoc solution, may be we could allow
    /// to override whole init logic in implementations.
    fn startup(&mut self, _pgb: &mut PostgresBackend, _sm: &FeStartupPacket) -> Result<()> {
        Ok(())
    }

    /// Check auth md5
    fn check_auth_md5(&mut self, _pgb: &mut PostgresBackend, _md5_response: &[u8]) -> Result<()> {
        bail!("MD5 auth failed")
    }

    /// Check auth jwt
    fn check_auth_jwt(&mut self, _pgb: &mut PostgresBackend, _jwt_response: &[u8]) -> Result<()> {
        bail!("JWT auth failed")
    }
}

/// PostgresBackend protocol state.
/// XXX: The order of the constructors matters.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum ProtoState {
    Initialization,
    Encrypted,
    Authentication,
    Established,
    Closed,
}

#[derive(Clone, Copy)]
pub enum ProcessMsgResult {
    Continue,
    Break,
}

/// Always-writeable sock_split stream.
/// May not be readable. See [`PostgresBackend::take_stream_in`]
pub enum Stream {
    Unencrypted(BufReader<tokio::net::TcpStream>),
    Tls(Box<tokio_rustls::server::TlsStream<BufReader<tokio::net::TcpStream>>>),
    Broken,
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Broken => unreachable!(),
        }
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
            Self::Broken => unreachable!(),
        }
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Broken => unreachable!(),
        }
    }
}
impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Broken => unreachable!(),
        }
    }
}

pub struct PostgresBackend {
    stream: Stream,

    // Output buffer. c.f. BeMessage::write why we are using BytesMut here.
    // The data between 0 and "current position" as tracked by the bytes::Buf
    // implementation of BytesMut, have already been written.
    buf_out: BytesMut,

    pub state: ProtoState,

    md5_salt: [u8; 4],
    auth_type: AuthType,

    peer_addr: SocketAddr,
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
}

pub fn query_from_cstring(query_string: Bytes) -> Vec<u8> {
    let mut query_string = query_string.to_vec();
    if let Some(ch) = query_string.last() {
        if *ch == 0 {
            query_string.pop();
        }
    }
    query_string
}

// Cast a byte slice to a string slice, dropping null terminator if there's one.
fn cstr_to_str(bytes: &[u8]) -> Result<&str> {
    let without_null = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    std::str::from_utf8(without_null).map_err(|e| e.into())
}

impl PostgresBackend {
    pub fn new(
        socket: tokio::net::TcpStream,
        auth_type: AuthType,
        tls_config: Option<Arc<rustls::ServerConfig>>,
    ) -> std::io::Result<Self> {
        let peer_addr = socket.peer_addr()?;

        Ok(Self {
            stream: Stream::Unencrypted(BufReader::new(socket)),
            buf_out: BytesMut::with_capacity(10 * 1024),
            state: ProtoState::Initialization,
            md5_salt: [0u8; 4],
            auth_type,
            tls_config,
            peer_addr,
        })
    }

    pub fn get_peer_addr(&self) -> &SocketAddr {
        &self.peer_addr
    }

    /// Read full message or return None if connection is closed.
    pub async fn read_message(&mut self) -> Result<Option<FeMessage>> {
        use ProtoState::*;
        match self.state {
            Initialization | Encrypted => FeStartupPacket::read_fut(&mut self.stream).await,
            Authentication | Established => FeMessage::read_fut(&mut self.stream).await,
            Closed => Ok(None),
        }
    }

    /// Flush output buffer into the socket.
    pub async fn flush(&mut self) -> std::io::Result<()> {
        while self.buf_out.has_remaining() {
            let bytes_written = self.stream.write(self.buf_out.chunk()).await?;
            self.buf_out.advance(bytes_written);
        }
        self.buf_out.clear();
        Ok(())
    }

    /// Write message into internal output buffer.
    pub fn write_message(&mut self, message: &BeMessage<'_>) -> Result<&mut Self, std::io::Error> {
        BeMessage::write(&mut self.buf_out, message)?;
        Ok(self)
    }

    /// Returns an AsyncWrite implementation that wraps all the data written
    /// to it in CopyData messages, and writes them to the connection
    ///
    /// The caller is responsible for sending CopyOutResponse and CopyDone messages.
    pub fn copyout_writer(&mut self) -> CopyDataWriter {
        CopyDataWriter { pgb: self }
    }

    /// A polling function that tries to write all the data from 'buf_out' to the
    /// underlying stream.
    fn poll_write_buf(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        while self.buf_out.has_remaining() {
            match Pin::new(&mut self.stream).poll_write(cx, &self.buf_out.chunk()) {
                Poll::Ready(Ok(bytes_written)) => {
                    self.buf_out.advance(bytes_written);
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending
            }
        }
        return Poll::Ready(Ok(()));
    }

    fn poll_flush(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    // Wrapper for run_message_loop() that shuts down socket when we are done
    pub async fn run<F, S>(mut self, handler: &mut impl Handler, shutdown_watcher: F) -> Result<()>
    where
        F: Fn() -> S,
        S: Future,
    {
        let ret = self.run_message_loop(handler, shutdown_watcher).await;
        let _ = self.stream.shutdown();
        ret
    }

    async fn run_message_loop<F, S>(
        &mut self,
        handler: &mut impl Handler,
        shutdown_watcher: F,
    ) -> Result<()>
    where
        F: Fn() -> S,
        S: Future,
    {
        trace!("postgres backend to {:?} started", self.peer_addr);

        tokio::select!(
            biased;

            _ = shutdown_watcher() => {
                // We were requested to shut down.
                tracing::info!("shutdown request received during handshake");
                return Ok(())
            },

            result = async {
                while self.state < ProtoState::Established {
                    if let Some(msg) = self.read_message().await? {
                        trace!("got message {msg:?} during handshake");

                        match self.process_handshake_message(handler, msg).await? {
                            ProcessMsgResult::Continue => {
                                self.flush().await?;
                                continue;
                            }
                            ProcessMsgResult::Break => {
                                trace!("postgres backend to {:?} exited during handshake", self.peer_addr);
                                return Ok(());
                            }
                        }
                    } else {
                        trace!("postgres backend to {:?} exited during handshake", self.peer_addr);
                        return Ok(());
                    }
                }
                Ok::<(), anyhow::Error>(())
            } => {
                // Handshake complete.
                result?;
            }
        );

        // Authentication completed
        let mut query_string = Bytes::new();
        while let Some(msg) = tokio::select!(
            biased;
            _ = shutdown_watcher() => {
                // We were requested to shut down.
                tracing::info!("shutdown request received in run_message_loop");
                Ok(None)
            },
            msg = self.read_message() => { msg },
        )? {
            trace!("got message {:?}", msg);

            let result = self.process_message(handler, msg, &mut query_string).await;
            self.flush().await?;
            match result? {
                ProcessMsgResult::Continue => {
                    self.flush().await?;
                    continue;
                }
                ProcessMsgResult::Break => break,
            }
        }

        trace!("postgres backend to {:?} exited", self.peer_addr);
        Ok(())
    }

    async fn start_tls(&mut self) -> anyhow::Result<()> {
        if let Stream::Unencrypted(plain_stream) =
            std::mem::replace(&mut self.stream, Stream::Broken)
        {
            let acceptor = TlsAcceptor::from(self.tls_config.clone().unwrap());
            let tls_stream = acceptor.accept(plain_stream).await?;

            self.stream = Stream::Tls(Box::new(tls_stream));
            return Ok(());
        };
        bail!("TLS already started");
    }

    async fn process_handshake_message(
        &mut self,
        handler: &mut impl Handler,
        msg: FeMessage,
    ) -> Result<ProcessMsgResult> {
        assert!(self.state < ProtoState::Established);
        let have_tls = self.tls_config.is_some();
        match msg {
            FeMessage::StartupPacket(m) => {
                trace!("got startup message {m:?}");

                match m {
                    FeStartupPacket::SslRequest => {
                        debug!("SSL requested");

                        self.write_message(&BeMessage::EncryptionResponse(have_tls))?;
                        if have_tls {
                            self.start_tls().await?;
                            self.state = ProtoState::Encrypted;
                        }
                    }
                    FeStartupPacket::GssEncRequest => {
                        debug!("GSS requested");
                        self.write_message(&BeMessage::EncryptionResponse(false))?;
                    }
                    FeStartupPacket::StartupMessage { .. } => {
                        if have_tls && !matches!(self.state, ProtoState::Encrypted) {
                            self.write_message(&BeMessage::ErrorResponse("must connect with TLS"))?;
                            bail!("client did not connect with TLS");
                        }

                        // NB: startup() may change self.auth_type -- we are using that in proxy code
                        // to bypass auth for new users.
                        handler.startup(self, &m)?;

                        match self.auth_type {
                            AuthType::Trust => {
                                self.write_message(&BeMessage::AuthenticationOk)?
                                    .write_message(&BeMessage::CLIENT_ENCODING)?
                                    // The async python driver requires a valid server_version
                                    .write_message(&BeMessage::server_version("14.1"))?
                                    .write_message(&BeMessage::ReadyForQuery)?;
                                self.state = ProtoState::Established;
                            }
                            AuthType::MD5 => {
                                rand::thread_rng().fill(&mut self.md5_salt);
                                self.write_message(&BeMessage::AuthenticationMD5Password(
                                    self.md5_salt,
                                ))?;
                                self.state = ProtoState::Authentication;
                            }
                            AuthType::NeonJWT => {
                                self.write_message(&BeMessage::AuthenticationCleartextPassword)?;
                                self.state = ProtoState::Authentication;
                            }
                        }
                    }
                    FeStartupPacket::CancelRequest { .. } => {
                        self.state = ProtoState::Closed;
                        return Ok(ProcessMsgResult::Break);
                    }
                }
            }

            FeMessage::PasswordMessage(m) => {
                trace!("got password message '{:?}'", m);

                assert!(self.state == ProtoState::Authentication);

                match self.auth_type {
                    AuthType::Trust => unreachable!(),
                    AuthType::MD5 => {
                        let (_, md5_response) = m.split_last().context("protocol violation")?;

                        if let Err(e) = handler.check_auth_md5(self, md5_response) {
                            self.write_message(&BeMessage::ErrorResponse(&e.to_string()))?;
                            bail!("auth failed: {}", e);
                        }
                    }
                    AuthType::NeonJWT => {
                        let (_, jwt_response) = m.split_last().context("protocol violation")?;

                        if let Err(e) = handler.check_auth_jwt(self, jwt_response) {
                            self.write_message(&BeMessage::ErrorResponse(&e.to_string()))?;
                            bail!("auth failed: {}", e);
                        }
                    }
                }
                self.write_message(&BeMessage::AuthenticationOk)?
                    .write_message(&BeMessage::CLIENT_ENCODING)?
                    .write_message(&BeMessage::ReadyForQuery)?;
                self.state = ProtoState::Established;
            }

            _ => {
                self.state = ProtoState::Closed;
                return Ok(ProcessMsgResult::Break);
            }
        }
        Ok(ProcessMsgResult::Continue)
    }

    async fn process_message(
        &mut self,
        handler: &mut impl Handler,
        msg: FeMessage,
        unnamed_query_string: &mut Bytes,
    ) -> Result<ProcessMsgResult> {
        // Allow only startup and password messages during auth. Otherwise client would be able to bypass auth
        // TODO: change that to proper top-level match of protocol state with separate message handling for each state
        assert!(self.state == ProtoState::Established);

        match msg {
            FeMessage::StartupPacket(_) | FeMessage::PasswordMessage(_) => {
                bail!("protocol violation");
            }

            FeMessage::Query(body) => {
                // remove null terminator
                let query_string = cstr_to_str(&body)?;

                trace!("got query {:?}", query_string);
                // xxx distinguish fatal and recoverable errors?
                if let Err(e) = handler.process_query(self, query_string).await {
                    // ":?" uses the alternate formatting style, which makes anyhow display the
                    // full cause of the error, not just the top-level context + its trace.
                    // We don't want to send that in the ErrorResponse though,
                    // because it's not relevant to the compute node logs.
                    error!("query handler for '{}' failed: {:?}", query_string, e);
                    self.write_message(&BeMessage::ErrorResponse(&e.to_string()))?;
                    // TODO: untangle convoluted control flow
                    if e.to_string().contains("failed to run") {
                        return Ok(ProcessMsgResult::Break);
                    }
                }
                self.write_message(&BeMessage::ReadyForQuery)?;
            }

            FeMessage::Parse(m) => {
                *unnamed_query_string = m.query_string;
                self.write_message(&BeMessage::ParseComplete)?;
            }

            FeMessage::Describe(_) => {
                self.write_message(&BeMessage::ParameterDescription)?
                    .write_message(&BeMessage::NoData)?;
            }

            FeMessage::Bind(_) => {
                self.write_message(&BeMessage::BindComplete)?;
            }

            FeMessage::Close(_) => {
                self.write_message(&BeMessage::CloseComplete)?;
            }

            FeMessage::Execute(_) => {
                let query_string = cstr_to_str(unnamed_query_string)?;
                trace!("got execute {:?}", query_string);
                // xxx distinguish fatal and recoverable errors?
                if let Err(e) = handler.process_query(self, query_string).await {
                    error!("query handler for '{}' failed: {:?}", query_string, e);
                    self.write_message(&BeMessage::ErrorResponse(&e.to_string()))?;
                }
                // NOTE there is no ReadyForQuery message. This handler is used
                // for basebackup and it uses CopyOut which doesn't require
                // ReadyForQuery message and backend just switches back to
                // processing mode after sending CopyDone or ErrorResponse.
            }

            FeMessage::Sync => {
                self.write_message(&BeMessage::ReadyForQuery)?;
            }

            FeMessage::Terminate => {
                return Ok(ProcessMsgResult::Break);
            }

            // We prefer explicit pattern matching to wildcards, because
            // this helps us spot the places where new variants are missing
            FeMessage::CopyData(_) | FeMessage::CopyDone | FeMessage::CopyFail => {
                bail!("unexpected message type: {:?}", msg);
            }
        }

        Ok(ProcessMsgResult::Continue)
    }
}



///
/// A futures::AsyncWrite implementation that wraps all data written to it in CopyData
/// messages.
///

pub struct CopyDataWriter<'a> {
    pgb: &'a mut PostgresBackend,
}

impl<'a> AsyncWrite for CopyDataWriter<'a> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = self.get_mut();

        match this.pgb.poll_write_buf(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }

        // CopyData
        // FIXME: if the input is large, we should split it into multiple messages.
        // Not sure what the threshold should be, but the ultimate hard limit is that
        // the length cannot exceed u32.
        // FIXME: flush isn't really required, but makes it easier
        // to view in wireshark
        this.pgb.write_message(&BeMessage::CopyData(buf))?;

        return Poll::Ready(Ok(buf.len()));
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();
        match this.pgb.poll_write_buf(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }
        this.pgb.poll_flush(cx)
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();
        match this.pgb.poll_write_buf(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }
        this.pgb.poll_flush(cx)
    }
}
