use std::fmt::Debug;
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures_util::{Sink, Stream};
use pin_project::pin_project;
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_serde::formats::SymmetricalBincode;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[pin_project]
pub struct BincodeTransport<Req, Resp, R, W>
where
    Req: DeserializeOwned + Debug,
    Resp: Serialize + Debug,
    R: AsyncRead,
    W: AsyncWrite,
{
    #[pin]
    stream: SymmetricallyFramed<FramedRead<R, LengthDelimitedCodec>, Req, SymmetricalBincode<Req>>,
    #[pin]
    sink: SymmetricallyFramed<FramedWrite<W, LengthDelimitedCodec>, Resp, SymmetricalBincode<Resp>>,
}

impl<Req, Resp, R, W> BincodeTransport<Req, Resp, R, W>
where
    Req: DeserializeOwned + Debug,
    Resp: Serialize + Debug,
    R: AsyncRead,
    W: AsyncWrite,
{
    pub fn new(read: R, write: W) -> Self {
        let length_delimited_read = FramedRead::new(read, LengthDelimitedCodec::new());
        let stream =
            SymmetricallyFramed::new(length_delimited_read, SymmetricalBincode::<Req>::default());

        let length_delimited_write = FramedWrite::new(write, LengthDelimitedCodec::new());
        let sink = tokio_serde::SymmetricallyFramed::new(
            length_delimited_write,
            SymmetricalBincode::<Resp>::default(),
        );

        Self { sink, stream }
    }
}

// forward to Self::stream
impl<Req, Resp, R, W> Stream for BincodeTransport<Req, Resp, R, W>
where
    Req: DeserializeOwned + Debug,
    Resp: Serialize + Debug,
    R: AsyncRead,
    W: AsyncWrite,
{
    type Item = io::Result<Req>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

// forward to Self::sink
impl<Req, Resp, R, W> Sink<Resp> for BincodeTransport<Req, Resp, R, W>
where
    Req: DeserializeOwned + Debug,
    Resp: Serialize + Debug,
    R: AsyncRead,
    W: AsyncWrite,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sink.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Resp) -> Result<(), Self::Error> {
        self.project().sink.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sink.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        self.project().sink.poll_close(cx)
    }
}
