package tech.mta.seeknal.connector

import java.io.IOException

import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaMetadata}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException

private class MockSchemaRegistry extends MockSchemaRegistryClient {

  /** MockSchemaRegistryClient is throwing different Exception than the mocked client, this is a workaround //
    * scalastyle:ignore
    *
    * Copied from https://github.com/AbsaOSS/ABRiS/blob/master/
    * src/test/scala/za/co/absa/abris/avro/AbrisMockSchemaRegistryClient.scala // scalastyle:ignore
    */
  @throws[IOException]
  @throws[RestClientException]
  override def getLatestSchemaMetadata(subject: String): SchemaMetadata = {
    try (super.getLatestSchemaMetadata(subject))
    catch {
      case e: IOException if e.getMessage == "No schema registered under subject!" =>
        throw new RestClientException("No schema registered under subject!", 404, 40401)
    }
  }
}
