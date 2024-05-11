package io.airbyte.integrations.destination.bigquery.operation

import com.google.cloud.bigquery.BigQuery
import io.airbyte.cdk.integrations.destination.record_buffer.SerializableBuffer
import io.airbyte.integrations.base.destination.typing_deduping.StreamId
import io.airbyte.integrations.destination.bigquery.typing_deduping.BigQueryDestinationHandler
import io.airbyte.integrations.destination.bigquery.typing_deduping.BigQuerySqlGenerator
import io.github.oshai.kotlinlogging.KotlinLogging

private val log = KotlinLogging.logger {}
class BigQueryDirectLoadingStorageOperaitons(
    bigQuery: BigQuery,
    sqlGenerator: BigQuerySqlGenerator,
    destinationHandler: BigQueryDestinationHandler,
    datasetLocation: String
) : BigQueryStorageOperations(bigQuery, sqlGenerator, destinationHandler, datasetLocation) {
    override fun writeToStage(streamId: StreamId, buffer: SerializableBuffer) {
        log.info { "Directly loading data to " }
    }
}
