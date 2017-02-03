package scorex.api.http.assets

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.{FreeSpec, Matchers}
import scorex.settings.Settings
import scorex.transaction.SimpleTransactionModule.StoredInBlock
import scorex.transaction.TransactionModule


class AssetsBroadcastApiRouteSpecification extends FreeSpec with Matchers with ScalatestRouteTest with PathMockFactory {
  "/assets/broadcast/issue" ignore {
    val stmMock = mock[TransactionModule[StoredInBlock]]
    val abar = AssetsBroadcastApiRoute(Settings.empty, stmMock)
    val json =
      """{
        |"senderPublicKey":"4c4nAckNxsuafXcg4abFPJ6wBZB6PD7KVNkD1wbVxnxZ",
        |"fee":10000000,
        |"signature":"123",
        |"name":"",
        |"quantity":0,
        |"reissuable":false,
        |"decimals":10,
        |"description":"",
        |"timestamp":1
        |}""".stripMargin
    Post("/assets/broadcast/issue", HttpEntity(ContentTypes.`application/json`, json)) ~> abar.route ~> check {
      println(response)
      status shouldBe StatusCodes.OK
    }
  }
}
