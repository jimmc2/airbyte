/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.sync

import io.airbyte.cdk.integrations.destination.record_buffer.SerializableBuffer
import io.airbyte.integrations.base.destination.typing_deduping.StreamConfig
import io.airbyte.integrations.base.destination.typing_deduping.StreamId
import java.time.Instant
import java.util.Optional

interface StorageOperations {
    /*
     *  ==================== Staging Operations ================================
     */

    /**
     * Prepare staging area which cloud be creating any object storage, temp tables or file storage
     */
    fun prepareStage(streamConfig: StreamConfig)

    /** Delete previously staged data, using deterministic information from streamId. */
    fun cleanupStage(streamId: StreamId)

    /** Copy data from provided buffer into stage. */
    fun writeToStage(streamId: StreamId, buffer: SerializableBuffer)

    /*
     *  ==================== Final Table Operations ================================
     */

    /** Create final schema extracted from [StreamId] */
    fun createFinalSchema(streamId: StreamId)

    /** Create final table extracted from [StreamId] */
    fun createFinalTable(streamConfig: StreamConfig, suffix: String, replace: Boolean)

    /**
     */
    fun softResetFinalTable(streamConfig: StreamConfig)

    /**
     * Attempt to atomically swap the final table (name and namespace extracted from [StreamId]).
     * This could be destination specific, INSERT INTO..SELECT * and DROP TABLE OR CREATE OR REPLACE
     * ... SELECT *, DROP TABLE
     */
    fun overwriteFinalTable(streamConfig: StreamConfig, tmpTableSuffix: String)

    /**
     */
    fun typeAndDedupe(
        streamConfig: StreamConfig,
        maxProcessedTimestamp: Optional<Instant>,
        finalTableSuffix: String
    )
}
