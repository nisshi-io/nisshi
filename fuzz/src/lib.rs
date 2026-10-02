use bytes::Bytes;
use nisshi_sans_io::{ApiKey, Error, Frame, Header, Request, Response, RootMessageMeta};

fn api_version<T>(_: &T) -> i16
where
    T: ApiKey,
{
    RootMessageMeta::messages()
        .requests()
        .iter()
        .find(|(api_key, _)| **api_key == T::KEY)
        .map(|(_, meta)| meta.version.valid.end)
        .expect("missing metadata")
}

/// Round-trips `request` through encode/decode twice at the request's
/// `api_version`, and asserts the two decodes agree with each other, rather
/// than asserting the first decode matches the pre-encode value.
///
/// A correct encoder/decoder pair is not required to be lossless against an
/// arbitrary in-memory value: a service may legitimately populate fields
/// spanning several version eras at once (trusting the encoder to drop
/// whatever doesn't apply at the negotiated version - e.g.
/// `FindCoordinatorResponse`'s v0-3 scalar fields alongside its v4+
/// `coordinators`), and every sequence field is generated as
/// `Option<Vec<T>>` regardless of whether it's actually nullable on the
/// wire, so a mandatory array left at `None` is wire-equivalent to
/// `Some(vec![])` but not `PartialEq`. Both legitimately make the first
/// decode differ from the original value without indicating a bug.
///
/// The first decode *is* a fixed point, though: re-encoding what the
/// decoder already normalized for this version must decode back to exactly
/// the same thing, which is the round-trip property a sans-io codec
/// actually has to satisfy.
pub fn check_request<T>(request: T) -> Result<(), Error>
where
    T: Request,
{
    let header = Header::Request {
        api_key: T::KEY,
        api_version: api_version(&request),
        correlation_id: 0,
        client_id: Some("fuzzer".into()),
    };

    let once = Frame::request(header.clone(), request.into())
        .and_then(Frame::request_from_bytes)
        .map(|frame| frame.body)?;

    let twice = Frame::request(header, once.clone())
        .and_then(Frame::request_from_bytes)
        .map(|frame| frame.body)?;

    assert_eq!(once, twice);

    Ok(())
}

pub fn encode<T>(request: T) -> Result<Bytes, Error>
where
    T: Request,
{
    Frame::request(
        Header::Request {
            api_key: T::KEY,
            api_version: api_version(&request),
            correlation_id: 0,
            client_id: Some("fuzzer".into()),
        },
        request.into(),
    )
}

/// The response counterpart of [`check_request`]; see its doc comment for
/// why this compares two decodes against each other rather than the first
/// decode against the pre-encode value.
pub fn check_response<T>(response: T) -> Result<(), Error>
where
    T: Response,
{
    let api_version = api_version(&response);

    let once = Frame::response(
        Header::Response { correlation_id: 0 },
        response.into(),
        T::KEY,
        api_version,
    )
    .and_then(|encoded| Frame::response_from_bytes(encoded, T::KEY, api_version))
    .map(|frame| frame.body)?;

    let twice = Frame::response(
        Header::Response { correlation_id: 0 },
        once.clone(),
        T::KEY,
        api_version,
    )
    .and_then(|encoded| Frame::response_from_bytes(encoded, T::KEY, api_version))
    .map(|frame| frame.body)?;

    assert_eq!(once, twice);

    Ok(())
}
