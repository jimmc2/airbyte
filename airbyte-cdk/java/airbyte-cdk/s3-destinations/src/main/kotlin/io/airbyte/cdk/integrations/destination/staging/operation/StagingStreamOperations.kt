/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.integrations.destination.staging.operation

import io.airbyte.cdk.integrations.base.JavaBaseConstants
import io.airbyte.cdk.integrations.destination.async.model.PartialAirbyteMessage
import io.airbyte.cdk.integrations.destination.record_buffer.FileBuffer
import io.airbyte.cdk.integrations.destination.record_buffer.SerializableBuffer
import io.airbyte.cdk.integrations.destination.s3.csv.CsvSerializedBuffer
import io.airbyte.cdk.integrations.destination.s3.csv.StagingDatabaseCsvSheetGenerator
import io.airbyte.commons.json.Jsons
import io.airbyte.integrations.base.destination.operation.AbstractStreamOperation
import io.airbyte.integrations.base.destination.operation.StorageOperations
import io.airbyte.integrations.base.destination.typing_deduping.DestinationInitialStatus
import io.airbyte.integrations.base.destination.typing_deduping.StreamConfig
import io.airbyte.integrations.base.destination.typing_deduping.migrators.MinimumDestinationState
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.stream.Stream
import org.apache.commons.io.FileUtils

class StagingStreamOperations<DestinationState : MinimumDestinationState>(
    private val storageOperations: StorageOperations<SerializableBuffer>,
    destinationInitialStatus: DestinationInitialStatus<DestinationState>,
    disableTypeDedupe: Boolean = false
) :
    AbstractStreamOperation<DestinationState, SerializableBuffer>(
        storageOperations,
        destinationInitialStatus,
        disableTypeDedupe
    ) {

    private val log = KotlinLogging.logger {}
    override fun writeRecords(streamConfig: StreamConfig, stream: Stream<PartialAirbyteMessage>) {
        val writeBuffer =
            CsvSerializedBuffer(
                FileBuffer(CsvSerializedBuffer.CSV_GZ_SUFFIX),
                StagingDatabaseCsvSheetGenerator(
                    JavaBaseConstants.DestinationColumns.V2_WITHOUT_META
                ),
                true
            )

        writeBuffer.use {
            stream.forEach { record: PartialAirbyteMessage ->
                it.accept(
                    record.serialized!!,
                    Jsons.serialize(record.record!!.meta),
                    record.record!!.emittedAt
                )
            }
            it.flush()
            log.info {
                "Buffer flush complete for stream ${streamConfig.id.originalName} (${FileUtils.byteCountToDisplaySize(it.byteCount)}) to staging"
            }
            storageOperations.writeToStage(streamConfig.id, writeBuffer)
        }
    }
}
