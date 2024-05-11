/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery.operation

import io.airbyte.cdk.integrations.destination.staging.operation.StagingStreamOperations
import io.airbyte.integrations.base.destination.operation.StreamOperation
import io.airbyte.integrations.base.destination.operation.StreamOperationsFactory
import io.airbyte.integrations.base.destination.typing_deduping.DestinationInitialStatus
import io.airbyte.integrations.destination.bigquery.migrators.BigQueryDestinationState

class BigQueryStreamOperationFactory(
    private val bigQueryGcsStorageOperations: BigQueryGcsStorageOperations
) : StreamOperationsFactory<BigQueryDestinationState> {
    override fun createInstance(
        destinationInitialStatus: DestinationInitialStatus<BigQueryDestinationState>
    ): StreamOperation<BigQueryDestinationState> {
        // TODO: Take required dependencies and build storageOperations rather than injecting.
        return StagingStreamOperations(bigQueryGcsStorageOperations, destinationInitialStatus)
    }
}
