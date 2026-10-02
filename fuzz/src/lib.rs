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

pub fn check_request<T>(request: T) -> Result<(), Error>
where
    T: Request + Clone,
{
    let encoded_and_decoded = Frame::request(
        Header::Request {
            api_key: T::KEY,
            api_version: api_version(&request),
            correlation_id: 0,
            client_id: Some("fuzzer".into()),
        },
        request.clone().into(),
    )
    .and_then(Frame::request_from_bytes)
    .map(|frame| frame.body)?;

    assert_eq!(encoded_and_decoded, request.into());

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

pub fn check_response<T>(response: T) -> Result<(), Error>
where
    T: Response + Clone,
{
    let api_version = api_version(&response);

    let encoded_and_decoded = Frame::response(
        Header::Response { correlation_id: 0 },
        response.clone().into(),
        T::KEY,
        api_version,
    )
    .and_then(|encoded| Frame::response_from_bytes(encoded, T::KEY, api_version))
    .map(|frame| frame.body)?;

    assert_eq!(encoded_and_decoded, response.into());

    Ok(())
}
