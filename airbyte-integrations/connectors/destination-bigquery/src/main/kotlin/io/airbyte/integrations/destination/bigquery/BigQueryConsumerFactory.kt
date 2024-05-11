/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery

import io.airbyte.cdk.integrations.destination.async.AsyncStreamConsumer
import io.airbyte.cdk.integrations.destination.async.buffers.BufferManager
import io.airbyte.cdk.integrations.destination.operation.SyncOperation
import io.airbyte.integrations.base.destination.operation.DefaultFlush
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import java.util.Optional
import java.util.function.Consumer

object BigQueryConsumerFactory {

    fun createStagingConsumer(
        outputRecordCollector: Consumer<AirbyteMessage>,
        syncOperation: SyncOperation,
        catalog: ConfiguredAirbyteCatalog,
        defaultNamespace: String
    ): AsyncStreamConsumer {
        // values here existed spread over the place
        // TODO: Find why max memory ratio is 0.4 capped
        return AsyncStreamConsumer(
            outputRecordCollector = outputRecordCollector,
            onStart = {},
            onClose = { _, streamSyncSummaries ->
                syncOperation.finalizeStreams(streamSyncSummaries)
            },
            onFlush = DefaultFlush(200 * 1024 * 1024, syncOperation),
            catalog = catalog,
            bufferManager =
                BufferManager(
                    (Runtime.getRuntime().maxMemory() * 0.4).toLong(),
                ),
            defaultNamespace = Optional.of(defaultNamespace),
        )
    }
}
