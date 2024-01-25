use super::{request::ApiPlural, Result};
use http::{Response, StatusCode};
use hyper::Body;
use kube_core::{ApiResource, Status};

/// Generate an "Invalid" kubernetes status response.
pub(crate) fn invalid(message: &str) -> Status {
    Status::failure(message, "Invalid").with_code(422)
}

/// Generate an "AlreadyExists" kubernetes status response.
pub(crate) fn already_exists(api_resource: &ApiResource, name: Option<&str>) -> Status {
    let resource_id = resource_id(&api_resource.group, &api_resource.kind, name);
    Status::failure(
        format!("{resource_id} already exists",).as_str(),
        "AlreadyExists",
    )
    .with_code(StatusCode::CONFLICT.as_u16())
}

/// Generate a "NotFound" kubernetes status response for a resource.
pub(crate) fn resource_not_found(api_plural: &ApiPlural) -> Status {
    Status::failure(&format!("resource {api_plural} not found"), "NotFound")
        .with_code(StatusCode::NOT_FOUND.as_u16())
}

/// Generate a "NotFound" kubernetes status response.
pub(crate) fn not_found(api_resource: &ApiResource, name: Option<&str>) -> Status {
    let resource_id = resource_id(&api_resource.group, &api_resource.kind, name);
    Status::failure(&format!("{resource_id} not found"), "NotFound")
        .with_code(StatusCode::NOT_FOUND.as_u16())
}

/// Generate a "MethodNotAllowed" kubernetes status response.
pub(crate) fn method_not_allowed(
    api_resource: &ApiResource,
    name: Option<String>,
    method: &str,
) -> Result<Response<Body>> {
    let resource_id = resource_id(&api_resource.group, &api_resource.kind, name.as_deref());
    let status = Status::failure(
        format!("method {method} not allowed for {resource_id}").as_str(),
        "MethodNotAllowed",
    )
    .with_code(StatusCode::METHOD_NOT_ALLOWED.as_u16());
    response(&status)
}

fn response(status: &Status) -> Result<Response<Body>> {
    let buf = serde_json::to_vec(status)?;
    Ok(Response::builder().status(status.code).body(buf.into())?)
}

fn resource_id(group: &str, kind: &str, name: Option<&str>) -> String {
    match (name, group.is_empty()) {
        (None, true) => format!("resource {kind}"),
        (None, false) => format!("resource {group}.{kind}"),
        (Some(name), true) => format!("{kind} {name}"),
        (Some(name), false) => format!("{group}.{kind} {name}"),
    }
}
