package io.airbyte.integrations.destination.bigquery.operation

import io.airbyte.cdk.integrations.destination.async.model.PartialAirbyteMessage
import io.airbyte.integrations.base.destination.operation.AbstractStreamOperation
import io.airbyte.integrations.base.destination.operation.StorageOperations
import io.airbyte.integrations.base.destination.typing_deduping.DestinationInitialStatus
import io.airbyte.integrations.base.destination.typing_deduping.StreamConfig
import io.airbyte.integrations.destination.bigquery.migrators.BigQueryDestinationState
import java.util.stream.Stream

class BigQueryLoadingStreamOperation(
    storageOperations: StorageOperations,
    destinationInitialStatus: DestinationInitialStatus<BigQueryDestinationState>
) : AbstractStreamOperation<BigQueryDestinationState>(storageOperations, destinationInitialStatus) {
    override fun writeRecords(streamConfig: StreamConfig, stream: Stream<PartialAirbyteMessage>) {
        TODO("Not yet implemented")
    }
}
