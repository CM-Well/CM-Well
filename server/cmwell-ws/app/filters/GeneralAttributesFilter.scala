package filters

import javax.inject._

import akka.stream.Materializer
import cmwell.ws.util.TypeHelpers
import play.api.libs.typedmap.TypedKey
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}
/**
  * Proj: server
  * User: gilad
  * Date: 10/24/17
  * Time: 8:04 AM
  */
class GeneralAttributesFilter @Inject()(implicit val mat: Materializer, ec: ExecutionContext) extends Filter with TypeHelpers {
  override def apply(f: RequestHeader => Future[Result])(rh: RequestHeader) = {
    f(rh.addAttr(Attrs.RequestReceivedTimestamp, System.currentTimeMillis())
        .addAttr(Attrs.UserIP, rh.headers.get("X-Forwarded-For").getOrElse(rh.remoteAddress)) // TODO: might be good to add ip to logs when user misbehaves
    )
  }
}

object Attrs {
  val RequestReceivedTimestamp: TypedKey[Long] = TypedKey.apply[Long]("RequestReceivedTimestamp")
  val UserIP:  TypedKey[String] = TypedKey.apply[String]("UserIP")
}