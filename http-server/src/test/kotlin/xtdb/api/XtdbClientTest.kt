package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb.openNode
import xtdb.api.query.Queries.from
import xtdb.api.tx.TxOps.putDocs
import java.net.URI
import java.net.URL

internal class XtdbClientTest {
    @Test
    fun startsRemoteNode() {
        openNode { httpServer() }.use { _ ->
            XtdbClient.openClient(URI("http://localhost:3000").toURL()).use { client ->
                client.submitTx(putDocs("foo", mapOf("xt/id" to "jms")))

                assertEquals(
                    listOf(mapOf("id" to "jms")),

                    client.openQuery(
                        from("foo") {
                            bindAll("xt/id" to "id")
                        }
                    ).use { it.toList() }
                )

                assertEquals(
                    listOf(mapOf("foo_id" to "jms")),

                    client.openQuery("SELECT foo.xt\$id AS foo_id FROM foo").use { it.toList() }
                )

                assertEquals(
                    listOf(mapOf("fooId" to "jms")),

                    client.openQuery(from("foo") { bind("xt/id", "fooId") }).use { it.toList() }
                )
            }
        }
    }
}
