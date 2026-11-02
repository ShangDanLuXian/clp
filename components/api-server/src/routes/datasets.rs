//! Routes that manage the clp-s storage engine's datasets.

use axum::Json;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use clp_rust_utils::dataset::DatasetName;
use serde::Deserialize;
use utoipa::IntoParams;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::client::Client;
use crate::routes::AppState;
use crate::routes::HandlerError;

/// Factory function.
///
/// # Returns
///
/// A newly created [`OpenApiRouter`] with all dataset routes registered.
pub fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(list_datasets))
        .routes(routes!(delete_dataset))
}

/// Path parameters for the dataset deletion endpoint.
#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Path)]
struct DatasetPath {
    /// The name of the dataset to delete.
    #[param(value_type = String)]
    dataset_name: DatasetName,
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
    delete,
    path = "/datasets/{dataset_name}",
    description = "Deletes a dataset: deletes its archives from the archive storage, and then \
        marks it as deleted. For filesystem archive storage, the archives are only deleted if the \
        dataset's archive storage path is a directory. Only the clp-s storage engine is \
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

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Method;
    use axum::http::Request;
    use axum::http::StatusCode;
    use clp_rust_utils::clp_config::package::config::Config;
    use clp_rust_utils::clp_config::package::config::Package;
    use clp_rust_utils::clp_config::package::config::StorageEngine;
    use clp_rust_utils::dataset::DatasetName;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::router;
    use crate::client::Client;
    use crate::routes::AppState;
    use crate::webui_client::WebuiClient;

    #[tokio::test]
    async fn dataset_deletion_route_rejects_invalid_dataset_name() -> anyhow::Result<()> {
        let config = Config {
            package: Package {
                storage_engine: StorageEngine::ClpS,
            },
            ..Config::default()
        };
        let sql_pool =
            sqlx::mysql::MySqlPoolOptions::new().connect_lazy("mysql://127.0.0.1:1/clp-db")?;
        let mongodb_client =
            mongodb::Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100")
                .await?;
        let (app, _) = router().split_for_parts();
        let app = app.with_state(AppState {
            client: Client::new(config.clone(), mongodb_client.clone(), sql_pool.clone()),
            webui_client: WebuiClient::new(config, sql_pool, mongodb_client).await,
        });

        let response = app
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
}
