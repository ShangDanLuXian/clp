//! Routes that manage the clp-s storage engine's datasets and their archives.

use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use clp_rust_utils::dataset::DatasetName;
use clp_rust_utils::types::ArchiveId;
use serde::Deserialize;
use utoipa::IntoParams;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::client::ArchiveDeletion;
use crate::client::Client;
use crate::client::DatasetCreation;
use crate::client::DeletedArchives;
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
#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Path)]
struct DatasetPath {
    /// The dataset's name.
    #[param(value_type = String)]
    dataset_name: DatasetName,
}

/// Query parameters for the archive listing endpoint.
#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
struct ArchiveListParams {
    /// The time range's lower bound (inclusive), in epoch milliseconds. Defaults to 0.
    #[serde(default)]
    begin_ts: i64,

    /// The time range's upper bound (inclusive), in epoch milliseconds. Omit for no upper bound.
    end_ts: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/datasets",
    description = "Gets the names of the datasets that aren't deleted. Only the clp-s storage \
        engine is supported.",
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
    description = "Creates a dataset, whose archives will be stored in its directory in the \
        archive storage. Only the clp-s storage engine is supported.",
    request_body(content = DatasetCreation),
    responses(
        (status = CREATED, description = "The dataset was created"),
        (
            status = BAD_REQUEST,
            description = "The request body can't be parsed (e.g., it isn't valid JSON); `name` \
                is an invalid dataset name; `retention_period_minutes` is 0; or the package's \
                storage engine isn't clp-s"
        ),
        (
            status = CONFLICT,
            description = "A dataset with the same name already exists and isn't deleted"
        ),
        (
            status = UNSUPPORTED_MEDIA_TYPE,
            description = "The request's `Content-Type` isn't `application/json`"
        ),
        (
            status = UNPROCESSABLE_ENTITY,
            description = "The request body doesn't match the schema (e.g., `name` is missing, a \
                field has the wrong JSON type or is out of range, or the body has an unknown field)"
        ),
        (status = INTERNAL_SERVER_ERROR)
    )
)]
async fn create_dataset(
    State(client): State<Client>,
    Json(creation): Json<DatasetCreation>,
) -> Result<StatusCode, HandlerError> {
    tracing::info!(dataset = % creation.name, "Creating the dataset.");
    client.create_dataset(&creation).await.inspect_err(|err| {
        tracing::error!(dataset = % creation.name, error = % err, "Failed to create the dataset.");
    })?;
    tracing::info!(dataset = % creation.name, "Created the dataset.");
    Ok(StatusCode::CREATED)
}

#[utoipa::path(
    delete,
    path = "/datasets/{dataset_name}",
    description = "Deletes a dataset: deletes its archives from the archive storage, and then \
        marks it as deleted. For filesystem archive storage, the archives are only deleted if the \
        dataset's path in the archive storage is a directory. Only the clp-s storage engine is \
        supported.",
    params(DatasetPath),
    responses(
        (status = NO_CONTENT, description = "The dataset was deleted"),
        (
            status = BAD_REQUEST,
            description = "Invalid dataset name, or the package's storage engine isn't clp-s"
        ),
        (status = NOT_FOUND, description = "The dataset doesn't exist or is deleted"),
        (
            status = INTERNAL_SERVER_ERROR,
            description = "Deleting the dataset's archives from the archive storage failed, in \
                which case the dataset isn't marked as deleted, or a database request failed"
        )
    )
)]
async fn delete_dataset(
    State(client): State<Client>,
    Path(DatasetPath { dataset_name }): Path<DatasetPath>,
) -> Result<StatusCode, HandlerError> {
    tracing::info!(dataset = % dataset_name, "Deleting the dataset.");
    client.delete_dataset(&dataset_name).await.inspect_err(|err| {
        tracing::error!(dataset = % dataset_name, error = % err, "Failed to delete the dataset.");
    })?;
    tracing::info!(dataset = % dataset_name, "Deleted the dataset.");
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/datasets/{dataset_name}/archives",
    description = "Gets the IDs of a dataset's archives whose timestamp ranges begin at or after \
        `begin_ts` and, if `end_ts` is given, end at or before `end_ts`. Only the clp-s storage \
        engine is supported.",
    params(DatasetPath, ArchiveListParams),
    responses(
        (
            status = OK,
            body = Vec<String>,
            description = "The archives' IDs, in no particular order"
        ),
        (
            status = BAD_REQUEST,
            description = "Invalid dataset name or query parameters (e.g., `end_ts` < \
                `begin_ts`), or the package's storage engine isn't clp-s"
        ),
        (status = NOT_FOUND, description = "The dataset doesn't exist or is deleted"),
        (status = INTERNAL_SERVER_ERROR)
    )
)]
async fn list_archives(
    State(client): State<Client>,
    Path(DatasetPath { dataset_name }): Path<DatasetPath>,
    Query(ArchiveListParams { begin_ts, end_ts }): Query<ArchiveListParams>,
) -> Result<Json<Vec<ArchiveId>>, HandlerError> {
    let ids = client
        .get_archive_ids(&dataset_name, begin_ts, end_ts)
        .await
        .inspect_err(|err| {
            tracing::error!(
                dataset = % dataset_name,
                error = % err,
                "Failed to list the dataset's archives."
            );
        })?;
    Ok(Json(ids))
}

#[utoipa::path(
    post,
    path = "/datasets/{dataset_name}/archives/delete",
    description = "Deletes a dataset's archives, selected either by their IDs or by a time range \
        that their timestamp ranges lie within (both bounds inclusive): deletes their metadata, \
        and then deletes them from the archive storage. For filesystem archive storage, an \
        archive is only deleted if its path is a directory. Only the clp-s storage engine is \
        supported.",
    params(DatasetPath),
    request_body(content = ArchiveDeletion),
    responses(
        (
            status = OK,
            body = DeletedArchives,
            description = "The deleted archives' IDs, in no particular order, and the requested \
                IDs that don't belong to any of the dataset's archives"
        ),
        (
            status = BAD_REQUEST,
            description = "Invalid dataset name; the request body can't be parsed (e.g., it \
                isn't valid JSON, or has both `ids` and `time_range`); `ids` is empty or has an \
                invalid archive ID; `end_ts` < `begin_ts`; or the package's storage engine isn't \
                clp-s"
        ),
        (status = NOT_FOUND, description = "The dataset doesn't exist or is deleted"),
        (
            status = UNSUPPORTED_MEDIA_TYPE,
            description = "The request's `Content-Type` isn't `application/json`"
        ),
        (
            status = UNPROCESSABLE_ENTITY,
            description = "The request body doesn't match the schema (e.g., a field is missing or \
                has the wrong JSON type, or the body has an unknown field)"
        ),
        (
            status = INTERNAL_SERVER_ERROR,
            description = "A database request failed, or deleting the archives from the archive \
                storage failed, in which case their metadata is already deleted"
        )
    )
)]
async fn delete_archives(
    State(client): State<Client>,
    Path(DatasetPath { dataset_name }): Path<DatasetPath>,
    Json(deletion): Json<ArchiveDeletion>,
) -> Result<Json<DeletedArchives>, HandlerError> {
    tracing::info!(dataset = % dataset_name, "Deleting the dataset's archives.");
    let deleted_archives = client
        .delete_archives(&dataset_name, deletion)
        .await
        .inspect_err(|err| {
            tracing::error!(
                dataset = % dataset_name,
                error = % err,
                "Failed to delete the dataset's archives."
            );
        })?;
    tracing::info!(
        dataset = % dataset_name,
        num_deleted_archives = deleted_archives.deleted_ids.len(),
        "Deleted the dataset's archives."
    );
    Ok(Json(deleted_archives))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Method;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::http::header::CONTENT_TYPE;
    use clp_rust_utils::clp_config::package::config::Config;
    use clp_rust_utils::clp_config::package::config::Package;
    use clp_rust_utils::clp_config::package::config::StorageEngine;
    use clp_rust_utils::dataset::DatasetName;
    use clp_rust_utils::types::ArchiveId;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::router;
    use crate::client::Client;
    use crate::routes::AppState;
    use crate::webui_client::WebuiClient;

    /// Builds the dataset routes' router for the clp-s storage engine, with clients whose `MySQL`
    /// and `MongoDB` servers are unreachable.
    ///
    /// # Returns
    ///
    /// The newly created router.
    ///
    /// # Panics
    ///
    /// Panics if the clients can't be created.
    async fn clp_s_app_with_unreachable_clients() -> axum::Router {
        let config = Config {
            package: Package {
                storage_engine: StorageEngine::ClpS,
            },
            ..Config::default()
        };
        let sql_pool = sqlx::mysql::MySqlPoolOptions::new()
            .connect_lazy("mysql://127.0.0.1:1/clp-db")
            .expect("the MySQL URI should be valid");
        let mongodb_client =
            mongodb::Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100")
                .await
                .expect("the MongoDB URI should be valid");
        let (app, _) = router().split_for_parts();
        app.with_state(AppState {
            client: Client::new(config.clone(), mongodb_client.clone(), sql_pool.clone()),
            webui_client: WebuiClient::new(config, sql_pool, mongodb_client).await,
        })
    }

    #[tokio::test]
    async fn dataset_creation_route_rejects_invalid_dataset_name() -> anyhow::Result<()> {
        let response = clp_s_app_with_unreachable_clients()
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/datasets")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"name": "bad-name"}"#))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = String::from_utf8(response.into_body().collect().await?.to_bytes().to_vec())?;
        let parse_error = "bad-name"
            .parse::<DatasetName>()
            .expect_err("name should be an invalid dataset name");
        assert_eq!(body, format!("Invalid input: {parse_error}"));
        Ok(())
    }

    #[tokio::test]
    async fn dataset_deletion_route_rejects_invalid_dataset_name() -> anyhow::Result<()> {
        let response = clp_s_app_with_unreachable_clients()
            .await
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/datasets/bad-name")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = String::from_utf8(response.into_body().collect().await?.to_bytes().to_vec())?;
        let parse_error = "bad-name"
            .parse::<DatasetName>()
            .expect_err("name should be an invalid dataset name");
        assert!(
            body.contains(&parse_error.to_string()),
            "unexpected body: {body}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn archive_listing_route_rejects_end_ts_before_begin_ts() -> anyhow::Result<()> {
        let response = clp_s_app_with_unreachable_clients()
            .await
            .oneshot(
                Request::builder()
                    .uri("/datasets/logs/archives?begin_ts=2&end_ts=1")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = String::from_utf8(response.into_body().collect().await?.to_bytes().to_vec())?;
        assert_eq!(body, "Invalid input: begin_ts must be <= end_ts");
        Ok(())
    }

    #[tokio::test]
    async fn archive_deletion_route_rejects_invalid_archive_id() -> anyhow::Result<()> {
        let response = clp_s_app_with_unreachable_clients()
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/datasets/logs/archives/delete")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"ids": ["not-a-uuid"]}"#))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = String::from_utf8(response.into_body().collect().await?.to_bytes().to_vec())?;
        let parse_error = "not-a-uuid"
            .parse::<ArchiveId>()
            .expect_err("the archive ID should be invalid");
        assert_eq!(body, format!("Invalid input: {parse_error}"));
        Ok(())
    }
}
