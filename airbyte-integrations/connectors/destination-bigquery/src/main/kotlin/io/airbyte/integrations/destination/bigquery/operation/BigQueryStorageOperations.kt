package io.airbyte.integrations.destination.bigquery.operation

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.TableId
import io.airbyte.cdk.integrations.destination.record_buffer.SerializableBuffer
import io.airbyte.cdk.integrations.util.ConnectorExceptionUtil
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.integrations.base.destination.operation.StorageOperations
import io.airbyte.integrations.base.destination.typing_deduping.StreamConfig
import io.airbyte.integrations.base.destination.typing_deduping.StreamId
import io.airbyte.integrations.base.destination.typing_deduping.TyperDeduperUtil
import io.airbyte.integrations.destination.bigquery.BigQueryUtils
import io.airbyte.integrations.destination.bigquery.formatter.BigQueryRecordFormatter
import io.airbyte.integrations.destination.bigquery.typing_deduping.BigQueryDestinationHandler
import io.airbyte.integrations.destination.bigquery.typing_deduping.BigQuerySqlGenerator
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant
import java.util.*

private val log = KotlinLogging.logger {}

abstract class BigQueryStorageOperations(
    private val bigquery: BigQuery,
    private val sqlGenerator: BigQuerySqlGenerator,
    private val destinationHandler: BigQueryDestinationHandler,
    private val datasetLocation: String
) : StorageOperations {
    private val existingSchemas = HashSet<String>()
    override fun prepareStage(streamId: StreamId, destinationSyncMode: DestinationSyncMode) {
        // Prepare staging schema
        createStagingSchema(streamId)
        // Prepare staging table. For overwrite, it does drop-create so we can skip explicit create.
        if (destinationSyncMode == DestinationSyncMode.OVERWRITE) {
            truncateStagingTable(streamId)
        } else {
            createStagingTable(streamId)
        }
    }

    private fun createStagingSchema(streamId: StreamId) {
        // create raw schema
        if (!existingSchemas.contains(streamId.rawNamespace)) {
            log.info { "Creating raw namespace ${streamId.rawNamespace}" }
            try {
                BigQueryUtils.getOrCreateDataset(
                    bigquery,
                    streamId.rawNamespace,
                    datasetLocation
                )
            } catch (e: BigQueryException) {
                if (ConnectorExceptionUtil.HTTP_AUTHENTICATION_ERROR_CODES.contains(e.code)) {
                    throw ConfigErrorException(e.message!!, e)
                } else {
                    throw e
                }
            }
            existingSchemas.add(streamId.rawNamespace)
        }
    }

    private fun createStagingTable(streamId: StreamId) {
        val tableId = TableId.of(streamId.rawNamespace, streamId.rawName)
        BigQueryUtils.createPartitionedTableIfNotExists(
            bigquery,
            tableId,
            BigQueryRecordFormatter.SCHEMA_V2
        )
    }

    private fun dropStagingTable(streamId: StreamId) {
        val tableId = TableId.of(streamId.rawNamespace, streamId.rawName)
        bigquery.delete(tableId)
    }

    /**
     * "Truncates" table, this is a workaround to the issue with TRUNCATE TABLE in BigQuery where
     * the table's partition filter must be turned off to truncate. Since deleting a table is a free
     * operation this option re-uses functions that already exist
     */
    private fun truncateStagingTable(streamId: StreamId) {
        val tableId = TableId.of(streamId.rawNamespace, streamId.rawName)
        log.info { "Truncating raw table $tableId" }
        dropStagingTable(streamId)
        createStagingTable(streamId)
    }

    override fun cleanupStage(streamId: StreamId) {
        log.info { "Nothing to cleanup in stage for Streaming inserts" }
    }

    abstract override fun writeToStage(streamId: StreamId, buffer: SerializableBuffer)

    override fun createFinalSchema(streamId: StreamId) {
        destinationHandler.execute(sqlGenerator.createSchema(streamId.finalNamespace))
    }

    override fun createFinalTable(streamConfig: StreamConfig, suffix: String, replace: Boolean) {
        destinationHandler.execute(sqlGenerator.createTable(streamConfig, suffix, replace))
    }

    override fun softResetFinalTable(streamConfig: StreamConfig) {
        TyperDeduperUtil.executeSoftReset(
            sqlGenerator = sqlGenerator,
            destinationHandler = destinationHandler,
            streamConfig,
        )
    }

    override fun overwriteFinalTable(streamConfig: StreamConfig, tmpTableSuffix: String) {
        if (tmpTableSuffix.isNotBlank()) {
            log.info {
                "Overwriting table ${streamConfig.id.finalTableId(BigQuerySqlGenerator.QUOTE)} with ${
                    streamConfig.id.finalTableId(
                        BigQuerySqlGenerator.QUOTE,
                        tmpTableSuffix,
                    )
                }"
            }
            destinationHandler.execute(
                sqlGenerator.overwriteFinalTable(streamConfig.id, tmpTableSuffix)
            )
        }
    }

    override fun typeAndDedupe(
        streamConfig: StreamConfig,
        maxProcessedTimestamp: Optional<Instant>,
        finalTableSuffix: String
    ) {
        TyperDeduperUtil.executeTypeAndDedupe(
            sqlGenerator = sqlGenerator,
            destinationHandler = destinationHandler,
            streamConfig,
            maxProcessedTimestamp,
            finalTableSuffix,
        )
    }
}
