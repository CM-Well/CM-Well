package cmwell.ws

import javax.inject._

import com.typesafe.scalalogging.LazyLogging
import controllers.NbgToggler
import ld.cmw.PassiveFieldTypesCacheTrait
import logic.CRUDServiceFS
import wsutil.FieldKey

import scala.concurrent.{ExecutionContext, Future}

class AggregateBothOldAndNewTypesCaches(crudService: CRUDServiceFS,
                                        tbg: NbgToggler) extends PassiveFieldTypesCacheTrait with LazyLogging {

  def crudServiceFS: CRUDServiceFS = crudService
  def nbg: Boolean = tbg.get

  lazy val oCache = crudService.obgPassiveFieldTypesCache
  lazy val nCache = crudService.nbgPassiveFieldTypesCache

  override def get(fieldKey: FieldKey, forceUpdateForType: Option[Set[Char]] = None)(implicit ec: ExecutionContext): Future[Set[Char]] = {
    val fo = oCache.get(fieldKey, forceUpdateForType)
    val fn = nCache.get(fieldKey, forceUpdateForType)
    for {
      o <- fo
      n <- fn
    } yield o union n
  }

  override def update(fieldKey: FieldKey, types: Set[Char])(implicit ec: ExecutionContext): Future[Unit] = {
    val fo = oCache.update(fieldKey, types)
    val fn = nCache.update(fieldKey, types)
    for {
      o <- fo
      n <- fn
    } yield ()
  }
}