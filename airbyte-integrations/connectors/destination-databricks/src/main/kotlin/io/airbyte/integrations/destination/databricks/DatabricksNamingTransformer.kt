/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.databricks

import io.airbyte.cdk.integrations.destination.NamingConventionTransformer
import io.airbyte.commons.text.Names

class DatabricksNamingTransformer : NamingConventionTransformer {
    override fun getIdentifier(name: String): String {
        return convertStreamName(name)
    }

    override fun getNamespace(namespace: String): String {
        return convertStreamName(namespace)
    }

    @Deprecated("Use getIdentifier")
    override fun getRawTableName(name: String): String {
        TODO("Not yet implemented")
    }

    @Deprecated("Use getIdentifier")
    override fun getTmpTableName(name: String): String {
        TODO("Not yet implemented")
    }

    override fun convertStreamName(input: String): String {
        return applyDefaultCase(Names.toAlphanumericAndUnderscore(input))
    }

    override fun applyDefaultCase(input: String): String {
        // Preserve casing as we are using quoted strings for all identifiers.
        return input
    }
}
