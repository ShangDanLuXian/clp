# Using the API server

CLP includes an API server that provides a RESTful interface for interacting with CLP.

## Starting the API server

CLP starts the API server based on the `api_server` section in `etc/clp-config.yaml`, which includes
a default configuration. You can uncomment and modify this section to override the defaults.

## API reference

All available API endpoints are defined in the [OpenAPI] Specification. You can explore the API
using [Swagger UI][swagger-ui].

:::{note}
`clp-text` doesn't support buffering search results in the file system or S3. When using
`clp-text`, set `buffer_results_in_mongodb` to `true` so that results are buffered in `MongoDB`
instead.
:::

## Example: Submitting search queries and receiving results

The API server exposes endpoints to submit search queries, and returns search results as a
continuous stream using [Server-sent Events][server-sent-events].

Assuming the server is running on the default host and port (`localhost:3001`), you can use the
following commands to submit a query to clp-json and stream the results.

1. Submit a search query:

   ```shell
   curl -X POST http://localhost:3001/query \
       -H "Content-Type: application/json" \
       -d '{
          "query_string": "*log*",
          "datasets": ["default"],
          "ignore_case": false,
          "max_num_results": 100
       }'
   ```

   Omit `max_num_results` to return all matching results. When specified, the limit must be a
   positive integer; zero is rejected.

   On success, the server responds with:

    ```json
    {
      "query_results_uri": "query_results/100"
    }
    ```

2. Retrieve search results:
   Use the returned `query_results_uri` to receive search results as an SSE stream:

   ```bash
   curl -N http://localhost:3001/query_results/100
   ```

   Example streamed output:

   ```text
   data: {"timestamp": 1767225600000, "message": "Example log message"}

   data: {"timestamp": 1767225600010, "message": "Another matched log line"}

   data: {"timestamp": 1767225600020, "message": "No logs found" }
   ```

## Example: Managing datasets and archives

When using clp-json, the API server manages datasets and their archives. These endpoints aren't
supported for clp-text.

Assuming the server is running on the default host and port (`localhost:3001`):

1. Create a dataset. Logs can only be compressed into datasets that exist, including the `default`
   dataset:

   ```shell
   curl -X POST http://localhost:3001/datasets \
       -H "Content-Type: application/json" \
       -d '{"name": "default", "retention_period_minutes": 43200}'
   ```

   `retention_period_minutes` is optional. When specified, archives in the dataset are deleted
   automatically once they're older than the retention period (see
   [Configuring retention periods][retention]). Otherwise, they're kept until they're deleted
   manually.

2. List the datasets:

   ```shell
   curl http://localhost:3001/datasets
   ```

3. List a dataset's archives, optionally only those whose timestamps lie within a time range (in
   milliseconds since the Unix epoch):

   ```shell
   curl -G http://localhost:3001/datasets/default/archives \
       -d "begin_timestamp_millisecs=<begin>" \
       -d "end_timestamp_millisecs=<end>"
   ```

4. Delete a dataset's archives, either by ID or by a time range that their timestamps lie within:

   ```shell
   curl -X POST http://localhost:3001/datasets/default/archives/delete \
       -H "Content-Type: application/json" \
       -d '{"archive_ids": ["<archive-id-0>", "<archive-id-1>"]}'
   ```

   ```shell
   curl -X POST http://localhost:3001/datasets/default/archives/delete \
       -H "Content-Type: application/json" \
       -d '{"begin_timestamp_millisecs": <begin>, "end_timestamp_millisecs": <end>}'
   ```

5. Delete a dataset, including all of its archives:

   ```shell
   curl -X DELETE http://localhost:3001/datasets/default
   ```

Deleted datasets and archives are no longer listed or searched, but their files are only removed
from the archive storage by the `archive-garbage-collector` service on its next sweep (every
`garbage_collector.sweep_interval.archive` minutes). A deleted dataset's name can't be reused until
then.

:::{warning}
When deleting a dataset, all files under the dataset's storage directory (`key_prefix` in object
storage) are removed, including any non-archive files. Avoid storing non-archive files in the
archive storage directory.
:::

[OpenAPI]: https://swagger.io/specification/
[retention]: ./guides-retention.md
[server-sent-events]: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events
[swagger-ui]: https://petstore.swagger.io/?url=https://docs.yscope.com/clp/DOCS_VAR_CLP_GIT_REF/_static/generated/api-server-openapi.json
