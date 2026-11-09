//! Routes that manage the clp-s storage engine's datasets and their archives.

use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use clp_rust_utils::dataset::DatasetName;
use serde::Deserialize;
use utoipa::IntoParams;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::client::ArchiveIds;
use crate::client::ArchiveMetadata;
use crate::client::ArchiveSelection;
use crate::client::ArchiveTimeRange;
use crate::client::Client;
use crate::client::DatasetCreation;
use crate::routes::AppState;
use crate::routes::HandlerError;

/// Factory function.
///
/// # Returns
///
/// A newly created [`OpenApiRouter`] with all dataset routes registered.
pub fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(list_datasets, create_dataset))
        .routes(routes!(delete_dataset))
        .routes(routes!(list_archives))
        .routes(routes!(delete_archives))
}

/// Path parameters for the endpoints of a dataset.
///
/// # NOTE
///
/// * [`DatasetName`] deserializes through `Option<String>`, and axum's top-level path deserializer
///   doesn't support optional values, so extracting `Path<DatasetName>` directly fails for every
///   request. Deserializing the dataset name as a struct field avoids this.
#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Path)]
struct DatasetPath {
    /// The dataset's name.
    #[param(value_type = String)]
    dataset_name: DatasetName,
}

#[utoipa::path(
    get,
    path = "/datasets",
    description = "Gets the names of the live datasets. Only the clp-s storage engine is \
        supported.",
    responses(
        (status = OK, body = Vec<String>, description = "The datasets' names, ordered by name"),
        (status = BAD_REQUEST, description = "The package's storage engine isn't clp-s"),
        (status = INTERNAL_SERVER_ERROR)
    )
)]
async fn list_datasets(
    State(client): State<Client>,
) -> Result<Json<Vec<DatasetName>>, HandlerError> {
    let names = client.get_dataset_names().await.inspect_err(|err| {
        tracing::error!(error = % err, "Failed to list the datasets.");
    })?;
    Ok(Json(names))
}

#[utoipa::path(
    post,
    path = "/datasets",
    description = "Creates a dataset, whose archives are stored at its location in the package's \
        archive output. Only the clp-s storage engine is supported.",
    request_body(content = DatasetCreation),
    responses(
        (status = CREATED, description = "The dataset was created"),
        (
            status = BAD_REQUEST,
            description = "The request body isn't valid JSON, or the package's storage engine \
                isn't clp-s"
        ),
        (
            status = CONFLICT,
            description = "A dataset with the same name already exists, possibly pending deletion"
        ),
        (
            status = UNSUPPORTED_MEDIA_TYPE,
            description = "The request's `Content-Type` isn't `application/json`"
        ),
        (
            status = UNPROCESSABLE_ENTITY,
            description = "The request body doesn't match the schema, including when `name` is \
                missing or isn't a valid dataset name, `retention_period_minutes` isn't a positive \
                32-bit integer, or the body has an unknown field"
        ),
        (status = INTERNAL_SERVER_ERROR)
    )
)]
async fn create_dataset(
    State(client): State<Client>,
    Json(creation): Json<DatasetCreation>,
) -> Result<StatusCode, HandlerError> {
    let name = creation.name.clone();
    client.create_dataset(creation).await.inspect_err(|err| {
        tracing::error!(dataset = % name, error = % err, "Failed to create the dataset.");
    })?;
    tracing::info!(dataset = % name, "Created the dataset.");
    Ok(StatusCode::CREATED)
}

#[utoipa::path(
    delete,
    path = "/datasets/{dataset_name}",
    description = "Marks a live dataset as deleted. Its archives are deleted from the archive \
        storage afterwards, in the background. Only the clp-s storage engine is supported.",
    params(DatasetPath),
    responses(
        (status = ACCEPTED, description = "The dataset was marked as deleted"),
        (
            status = BAD_REQUEST,
            description = "Invalid dataset name, or the package's storage engine isn't clp-s"
        ),
        (status = NOT_FOUND, description = "There's no live dataset with the name"),
        (status = INTERNAL_SERVER_ERROR)
    )
)]
async fn delete_dataset(
    State(client): State<Client>,
    Path(DatasetPath { dataset_name }): Path<DatasetPath>,
) -> Result<StatusCode, HandlerError> {
    client
        .mark_dataset_deleted(&dataset_name)
        .await
        .inspect_err(|err| {
            tracing::error!(
                dataset = % dataset_name,
                error = % err,
                "Failed to mark the dataset as deleted."
            );
        })?;
    tracing::info!(dataset = % dataset_name, "Marked the dataset as deleted.");
    Ok(StatusCode::ACCEPTED)
}

#[utoipa::path(
    get,
    path = "/datasets/{dataset_name}/archives",
    description = "Gets a live dataset's live archives whose timestamp ranges lie within the given \
        time range. Only the clp-s storage engine is supported.",
    params(DatasetPath, ArchiveTimeRange),
    responses(
        (
            status = OK,
            body = Vec<ArchiveMetadata>,
            description = "The archives, in no particular order"
        ),
        (
            status = BAD_REQUEST,
            description = "Invalid dataset name or query parameters (e.g., \
                `begin_timestamp_millisecs` > `end_timestamp_millisecs`), or the package's storage \
                engine isn't clp-s"
        ),
        (status = NOT_FOUND, description = "There's no live dataset with the name"),
        (status = INTERNAL_SERVER_ERROR)
    )
)]
async fn list_archives(
    State(client): State<Client>,
    Path(DatasetPath { dataset_name }): Path<DatasetPath>,
    Query(time_range): Query<ArchiveTimeRange>,
) -> Result<Json<Vec<ArchiveMetadata>>, HandlerError> {
    let archives = client
        .get_dataset_archives(&dataset_name, time_range)
        .await
        .inspect_err(|err| {
            tracing::error!(
                dataset = % dataset_name,
                error = % err,
                "Failed to list the dataset's archives."
            );
        })?;
    Ok(Json(archives))
}

#[utoipa::path(
    post,
    path = "/datasets/{dataset_name}/archives/delete",
    description = "Marks a live dataset's live archives as deleted, selected either by their IDs \
        or by a time range that their timestamp ranges lie within. When selecting by IDs, no \
        archive is marked unless every ID is the ID of a live archive of the dataset. The archives \
        are deleted from the archive storage afterwards, in the background. Only the clp-s \
        storage engine is supported.",
    params(DatasetPath),
    request_body(content = ArchiveSelection),
    responses(
        (status = OK, body = ArchiveIds, description = "The IDs of the marked archives"),
        (
            status = BAD_REQUEST,
            description = "Invalid dataset name; the request body isn't valid JSON; \
                `begin_timestamp_millisecs` > `end_timestamp_millisecs`; or the package's storage \
                engine isn't clp-s"
        ),
        (
            status = NOT_FOUND,
            body = Option<ArchiveIds>,
            description = "There's no live dataset with the name, with no body; or some of the \
                requested IDs aren't the IDs of live archives of the dataset, with those IDs in \
                the body"
        ),
        (
            status = UNSUPPORTED_MEDIA_TYPE,
            description = "The request's `Content-Type` isn't `application/json`"
        ),
        (
            status = UNPROCESSABLE_ENTITY,
            description = "The request body doesn't match the schema, including when it has both \
                or neither of `archive_ids` and the time range, only one of the time range's \
                bounds, an invalid archive ID, or an unknown field"
        ),
        (status = INTERNAL_SERVER_ERROR)
    )
)]
async fn delete_archives(
    State(client): State<Client>,
    Path(DatasetPath { dataset_name }): Path<DatasetPath>,
    Json(selection): Json<ArchiveSelection>,
) -> Result<Json<ArchiveIds>, HandlerError> {
    let marked_archives = client
        .mark_dataset_archives_deleted(&dataset_name, selection)
        .await
        .inspect_err(|err| {
            tracing::error!(
                dataset = % dataset_name,
                error = % err,
                "Failed to mark the dataset's archives as deleted."
            );
        })?;
    tracing::info!(
        dataset = % dataset_name,
        num_archives = marked_archives.archive_ids.len(),
        "Marked the dataset's archives as deleted."
    );
    Ok(Json(marked_archives))
}
