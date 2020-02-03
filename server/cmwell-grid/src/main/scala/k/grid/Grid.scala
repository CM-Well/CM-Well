/**
  * © 2019 Refinitiv. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package k.grid

import java.io.File
import cmwell.util.concurrent._
import akka.actor.Actor.Receive
import akka.io.IO
import com.typesafe.scalalogging.LazyLogging
import k.grid.Config._
import akka.actor._
import akka.cluster.singleton._
import akka.remote.RemoteScope
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.pattern.ask
import akka.util.Timeout
import k.grid.dmap.api.DMaps
import k.grid.dmap.impl.inmem.InMemDMap
import k.grid.registration.{LocalRegistrationManager, RegistrationCoordinator}
import k.grid.monitoring._
import k.grid.service.{LocalServiceManager, ServiceCoordinator, ServiceTypes}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import akka.cluster.ClusterEvent.{ClusterDomainEvent, LeaderChanged, MemberUp}
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

case class WhoIAm(address: String)
case object WhoAreYou

object GridReceives {
  def monitoring(sender: () => ActorRef): Receive = {
    case WhoAreYou =>
      sender() ! WhoIAm(Grid.me.toString)
  }
}

case class GridConnection(
  memberName: String,
  clusterName: String = Config.clusterName,
  hostName: String = Config.host,
  port: Int = Config.port,
  seeds: Set[String] = Config.seeds,
  labels: Set[String] = Config.labels,
  persistentDmapDir: String = Config.dmapDataDir
)

object GridJvm {
  def apply(host: String, identityName: String): GridJvm = {
    val identity = Grid.jvmIdentities.find(_.name == identityName)
    GridJvm(host, identity)
  }

  def apply(hostname: String): GridJvm = {
    val splt = hostname.split(":")
    val host = splt(0)
    val identity = Grid.jvmIdentities.find(_.systemPort == splt(1).toInt)

    GridJvm(host, identity)
  }

  def apply(m: Member): GridJvm = {
    GridJvm(m.address.host.getOrElse("") + ":" + m.address.port.getOrElse(0).toString)
  }

  def apply(hr: JvmIdentity): GridJvm = {
    GridJvm(Grid.hostName, Some(hr))
  }

  def apply(host: String, identity: JvmIdentity): GridJvm = {
    GridJvm(host, Some(identity))
  }
}

//                                       todo: get rid of the option.
case class GridJvm(host: String, identity: Option[JvmIdentity]) {
  def port: Int = {
    if (identity.isEmpty) 0
    else if (identity.get.systemPort != 0) identity.get.systemPort
    else {
      val idName = identity.get.name

      LocalRegistrationManager.jvms
        .find(jvm => jvm.host == host && jvm.identity.isDefined && jvm.identity.get.name == idName)
        .map(_.identity.map(_.systemPort))
        .flatten
        .getOrElse(0)
    }
  }

  def jvmName = identity.map(_.name).getOrElse("")

  def hasLabel(label: String) = identity.map(_.labels.contains(label)).getOrElse(false)

  override def toString: String = hostname

  def hostname = {
    if (identity.isDefined)
      s"$host:$port"
    else host
  }

  def address = {
    identity match {
      case Some(r) => s"${r.name}@$host"
      case None    => host
    }
  }
}

trait SelectableSingleton {
  def deployedOn: JvmIdentity
  def name: String

  def localProxy: GridJvm = {
    GridJvm(deployedOn)
  }

  def remoteProxy(host: String): GridJvm = {
    GridJvm(host, deployedOn)
  }
}

object Grid extends LazyLogging {

  var extraDataCollector: () => String = { () =>
    ""
  }

  private var isClusterMember = false
  private def getFreePort: Int = {
    import java.net.ServerSocket
    val s = new ServerSocket(0)
    val port = s.getLocalPort
    s.close()
    port
  }

  def isController = isClusterMember

  var singletonJvm: GridJvm = _

  def isSingletonJvm = singletonJvm == thisMember

  private var gr = new GridImp()
  implicit val timeout = Timeout(5.seconds)
  def g = gr
  def system = g.system

  private var _clusterName: String = _
  def clusterName = _clusterName

  private var _hostName: String = _
  def hostName = _hostName

  private var _seeds: Set[String] = _
  def seeds = _seeds

  private var _port: Int = _
  def port = _port

  private val _roles: Set[String] = Set(akkaClusterRole)
  def roles = _roles

  private var _memberName: String = _
  def memberName = _memberName

  private var _labels: Set[String] = _
  def labels = _labels

  private var _persistentDmapDir: String = _
  def persistentDmapDir = _persistentDmapDir

  def setGridConnection(gridConnection: GridConnection): Unit = {
    _clusterName = gridConnection.clusterName
    _hostName = gridConnection.hostName
    _memberName = gridConnection.memberName
    _port = if (gridConnection.port == 0) getFreePort else gridConnection.port
    _seeds = gridConnection.seeds
    _labels = gridConnection.labels
    _persistentDmapDir = gridConnection.persistentDmapDir
  }

  private def systemMemberIdentity = JvmIdentity("ctrl", seeds.head.split(":")(1).toInt)

  def jvmIdentities: Set[JvmIdentity] = {
    val res = LocalRegistrationManager.jvms.flatMap(_.identity).map(jid => JvmIdentity(jid.name, 0))
    res + systemMemberIdentity
  }
  private var services: ServiceTypes = ServiceTypes()

  def declareServices(serviceTypes: ServiceTypes): Unit = scala.concurrent.blocking {
    synchronized {
      if (services.m.isEmpty) this.services = serviceTypes
      else {
        logger.warn(s"declaring services more than once [existing services: ${services.m.keys
          .mkString("[", ",", "]")}] [new services: ${serviceTypes.m.keys.mkString("[", ",", "]")}]")
        val newM = serviceTypes.m.filterKeys { serviceName =>
          this.services.m.get(serviceName).fold(true) { _ =>
            logger.error(s"Service: $serviceName is declared more than once")
            false
          }
        }
        this.services = ServiceTypes(this.services.m ++ newM)
      }
    }
  }

  //var label : String = "Grid"

  def thisJvmIdentity = JvmIdentity(memberName, port, labels)

  def thisMember = GridJvm(hostName, thisJvmIdentity)
  def seedMembers = seeds.map(GridJvm(_))

  def join = {
    isClusterMember = true
    g.join(clusterName, hostName, seeds, port, services, Some(roles))
  }
  def joinClient = g.joinClient(clusterName, hostName, seeds, port, services)

  def rejoin {
    shutdown
    gr = new GridImp()
    join
  }

  // TODO: to props or not to props
  def createAnon(props: Props) = g.createAnon(props)
  def create(props: Props, name: String): ActorRef = g.create(props, name)
  def createAnon(clazz: Class[_ <: Actor]): ActorRef = g.createAnon(clazz)
  def create(clazz: Class[_ <: Actor], name: String): ActorRef = g.create(clazz, name)
  def createAnon(clazz: Class[_ <: Actor], args: Any*) = g.createAnon(clazz, args: _*)
  def create(clazz: Class[_ <: Actor], name: String, args: Any*) = g.create(clazz, name, args: _*)
  def createSingleton(clazz: Class[_ <: Actor], name: String, role: Option[String], args: Any*) =
    g.createSingleton(clazz, name, role, args: _*)
  def selectSingleton(name: String, role: Option[String], proxy: GridJvm = thisMember) =
    g.selectSingleton(name, role, proxy)
  // TODO: change to address
  def create_remote(clazz: Class[_ <: Actor], name: String, hostname: String, port: Int, args: Any*): ActorRef =
    g.create_remote(clazz, name, hostname, port, args: _*)

  def createOnJvm(jvm: GridJvm, clazz: Class[_ <: Actor], name: String, args: Any*): ActorRef =
    g.createOnJvm(jvm, clazz, name, args: _*)
  def createSomewhere(white: Set[GridJvm],
                      black: Set[GridJvm],
                      clazz: Class[_ <: Actor],
                      name: String,
                      args: Any*): ActorRef = g.createSomewhere(white, black, clazz, name, args: _*)

  // TODO: remove this ASAP.
  @deprecated("marked by Gilad", "long ago")
  def selectByPath(path: String): ActorSelection = g.selectByPath(path)

  def selectActorAnywhere(name: String): ActorSelection = g.selectActorAnywhere(name)

  def selectActor(name: String, member: GridJvm) = g.selectActor(name, member)

  def allActors = {
    val responses = Grid.jvmsAll.map { member =>
      (Grid.selectActor(MonitorActor.name, member) ? PingChildren).mapTo[ActiveActors]
    }
    successes(responses)
  }

  def registerServices(st: ServiceTypes) = g.registerServices(st)
  def serviceRef(name: String) = g.serviceRef(name)

  def getChildren = g.getChildren
  def getSingletons = g.getSingletons

  def availableMachines = g.jvms.map(_.host)

  def upHosts = GridData.upHosts
  def downHosts = GridData.downHosts
  def allKnownHosts = GridData.allKnownHosts

  def jvms(hr: JvmIdentity) = g.jvms(hr)

  def jvmsOnHost(host: String) = {
    jvmsAll.filter(_.host == host)
  }

  def jvmsOnThisHost = {
    jvmsOnHost(Grid.hostName)
  }

  def jvmsAll = {
    /*val col = for{
      r <- Grid.jvmIdentities
    } yield {
      jvms(r)
    }
    col.flatten*/
    LocalRegistrationManager.jvms
  }

  def getRunningJvmsInfo = {
    val all = jvmsAll
    val futures = all.map { jvm =>
      (selectActor(ClientActor.name, jvm) ? GetClientInfo).mapTo[JvmInfo].map { ci =>
        jvm -> ci
      }
    }

    val future = cmwell.util.concurrent.successes(futures).map(_.toMap)
    future
  }

  def getAllJvmsInfo = {
    GridMonitor.getMembersInfo.map(_.m)
  }

  def getSingletonsInfo = {
    LocalServiceManager.mapping.map { tpl =>
      val jvmName = tpl._2.map(_.identity.map(_.name)).flatten.getOrElse("")
      val jvmHost = tpl._2.map(_.host).getOrElse("")
      val addr = tpl._2.map(_.hostname).getOrElse("")
      SingletonData(tpl._1, s"$jvmName@$jvmHost", addr)
    }.toSet
  }

  case object Resolve
  case object NoResponseAfterGraceTimeTimedOut
  class Resolver(actorSelection: ActorSelection, retries: Int, timeout: FiniteDuration) extends Actor {

    import scala.concurrent.ExecutionContext.Implicits.global

    require(retries > 0)

    def retry(retriesLeft: Int, client: ActorRef, cancellable: akka.actor.Cancellable): Receive = {
      case Resolve if retriesLeft > 1 => {
        actorSelection ! Identify(None)
        context.become(retry(retries - 1, client, cancellable))
      }
      case Resolve => {
        cancellable.cancel()
        context.system.scheduler.scheduleOnce(timeout * retries, self, NoResponseAfterGraceTimeTimedOut)
      }
      case ActorIdentity(_, Some(remoteRef)) => {
        client ! remoteRef
        cancellable.cancel()
        context.stop(self)
      }
      case ActorIdentity(_, None) => {
        client ! Status.Failure(
          new NoSuchElementException(
            s"could not resolve actor selection for actor $actorSelection. Actor doesn't exist."
          )
        )
        cancellable.cancel()
        context.stop(self)
      }
      case NoResponseAfterGraceTimeTimedOut => {
        client ! Status.Failure(
          new RuntimeException(s"No response (timeout) for actor selection of $actorSelection Identity request")
        )
        context.stop(self)
      }
    }

    def receive: Receive = {
      case Resolve => {
        actorSelection ! Identify(None)
        val c = context.system.scheduler.schedule(timeout, timeout, self, Resolve)
        context.become(retry(retries - 1, sender(), c))
      }
    }
  }

  def getRefFromSelection(actorSelection: ActorSelection,
                          retries: Int = 10,
                          timeout: FiniteDuration = 1.second): Future[ActorRef] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val ar = system.actorOf(Props(new Resolver(actorSelection, retries, timeout)))
    (ar ? Resolve)(akka.util.Timeout(timeout * retries * 3)).mapTo[ActorRef]
  }

  def shutdown: Unit = {
    if (isClusterMember) {
      g.cluster.leave(g.cluster.selfAddress)
      Thread.sleep(10000)
    }

    g.terminate
    g.singletonsProxies = Set.empty[String]
  }
  def leader: Address = g.leader
  def me: Address = g.me
  def members: Set[Address] = g.members
  def membersVerbose: Set[Member] = g.membersVerbose
  def rolesMembers(role: String): Vector[Address] = g.rolesMembers(role)
  def actors(name: String, role: Option[String] = None): Vector[ActorSelection] = g.actors(name, role)

  /*****only if you want to create another instance need to be removed **********************/
  def apply(): Grid = new GridImp()

  def gridName: String = g.gridName

  def task(name: String)(t: => Unit): ActorRef = task(name, 5000, 5000, None)(t)

  def task(name: String, start: Long, interval: Long, role: Option[String])(t: => Unit): ActorRef = {
    createSingleton(classOf[TaskActor], name, role, start, interval, () => t)
  }

  def clusterProxy: GridJvm = {
    if (isController) Grid.thisMember else Grid.seedMembers.head
  }

  def subscribeForGridEvents(subscriber: ActorRef) = g.subscribeForGridEvents(subscriber)

//  def pauseTask(name : String, role : Option[String] = None) {
//    selectSingleton(name, role) ! PauseTask
//  }
//
//  def resumeTask(name : String, role : Option[String] = None) {
//    selectSingleton(name, role) ! ResumeTask
//  }
//
//  def stopTask(name : String , role : Option[String] = None) {
//    selectSingleton(name, role) ! PoisonPill.getInstance
//  }
}

trait Grid {
  def join(clusterName: String,
           hostname: String,
           seeds: Set[String],
           port: Int,
           singletons: ServiceTypes,
           roles: Option[Set[String]]): Unit
  def joinClient(systemName: String, hostname: String, seeds: Set[String], port: Int, singletons: ServiceTypes): Unit
  //def join( clusterName : String, hostname : String , seeds : Set[String] , roles : Option[Set[String]]) : Unit
  def create(props: Props, name: String): ActorRef
  def create(clazz: Class[_ <: Actor], name: String): ActorRef
  def create(clazz: Class[_ <: Actor], name: String, args: Any*): ActorRef
  def createOnJvm(jvm: GridJvm, clazz: Class[_ <: Actor], name: String, seq: Any*): ActorRef
  def createSomewhere(white: Set[GridJvm],
                      black: Set[GridJvm],
                      clazz: Class[_ <: Actor],
                      name: String,
                      seq: Any*): ActorRef
  def createSingleton(clazz: Class[_ <: Actor], name: String, role: Option[String], args: Any*): ActorRef
  def selectSingleton(name: String, role: Option[String], proxy: GridJvm): ActorSelection
  def create_remote(clazz: Class[_ <: Actor], name: String, hostname: String, port: Int, args: Any*): ActorRef

  def selectByPath(path: String): ActorSelection
  def selectActorAnywhere(path: String): ActorSelection

  def registerServices(st: ServiceTypes): Unit
  def serviceRef(name: String): ActorRef
  def selectActor(name: String, member: GridJvm): ActorSelection

  def run(clazz: Class[_ <: Actor], name: String, role: String): ActorRef
  def members: Set[Address]
  def rolesMembers(role: String): Vector[Address]
  def leader: Address
  def me: Address
  def shutdown: Unit
  def actors(name: String, role: Option[String] = None): Vector[ActorSelection]
  def gridName: String
  def getChildren: Set[String]
  def jvms: Set[GridJvm]
  def jvms(hr: JvmIdentity): Set[GridJvm]
  def getSingletons: Map[String, Option[String]]
  protected var singletons = Map.empty[String, Option[String]]
  protected var singletonsProxies = Set.empty[String]

  def subscribeForGridEvents(subscriber: ActorRef)
}

trait NodeMember

case class JvmIdentity(name: String, systemPort: Int, labels: Set[String] = Set.empty[String])

class GridImp extends Grid with LazyLogging {

  import akka.util.Timeout

  import scala.concurrent.duration._
  import akka.util.Timeout
  implicit val timeout = Timeout(1 second)

  //var clusterSystemName = "LeprechaunSystem"
  var clusterSystemName: String = _
  var system: ActorSystem = _
  var cluster: Cluster = _
  var address: Address = _
  var internalActor: ActorRef = _
  var roles: Option[Set[String]] = None
  var seeds: Set[String] = Set.empty[String]
  var children: Set[String] = Set.empty[String]

  private def addChild(name: String) = children = children + name

  def jvms: Set[GridJvm] = {
    GridData.upHosts.map { h =>
      GridJvm(h, None)
    }
  }

  def jvms(hr: JvmIdentity): Set[GridJvm] = {
    jvms.map { host =>
      host.copy(identity = Some(hr))
    }
  }

  def getChildren: Set[String] = children

  // check what members has a specific role
  def rolesMembers(role: String): Vector[Address] = {
    val cluster = Cluster(system)

    val status = cluster.state.members

    val nodes = status.collect {
      case m if (m.status == MemberStatus.Up && m.hasRole(role)) => m.address
    }
    nodes.toVector
  }

  // TODO: chnage sortedset
  def members: Set[Address] = {
    val cluster = Cluster(system)
    val status = cluster.state.members
//    val nodes = status.collect {
//      case m if m.status == MemberStatus.Up => m.address
//    }

    val nodes = status.filter(mem => mem.status == MemberStatus.Up).map(_.address)
    nodes
  }

  def membersVerbose: Set[Member] = {
    val cluster = Cluster(system)
    cluster.state.members
  }

  def leader: Address = {
    val cluster = Cluster(system)
    val leader = cluster.state.getLeader
    leader
  }

  def me: Address = {
    Address("akka.tcp", clusterName, Grid.hostName, Grid.port)
  }

  def joinClient(clusterName: String,
                 hostname: String,
                 seeds: Set[String],
                 port: Int,
                 singletons: ServiceTypes): Unit = {
    logger.info(s"############### Connecting to port $port ###################")
    val confTemp =
      ConfigFactory
        .parseString("akka.remote.netty.tcp.port=%d".format(port))
        .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=%s".format(hostname)))

    system = ActorSystem(
      clusterName,
      confTemp.withFallback(config.getConfig("cmwell.grid").withOnlyPath("akka").withFallback(config))
    )

    clusterSystemName = clusterName
    //protocol : scala.Predef.String, system : scala.Predef.String, host : scala.Predef.String, port : scala.Int

    val monitorActor = system.actorOf(Props[MonitorActor], name = MonitorActor.name)
//    IO(Http)(Grid.system) ! Http.Bind(monitorActor, interface = hostname, port = monitorPort)

    Grid.create(classOf[ClientActor], ClientActor.name)
    Grid.create(classOf[LocalRegistrationManager], LocalRegistrationManager.name)
    registerServices(singletons)
    DMaps.get.foreach(_.initSlave)
  }

  def join(clusterName: String,
           hostname: String,
           seeds: Set[String],
           port: Int,
           singletons: ServiceTypes,
           roles: Option[Set[String]]): Unit = {
    // TODO: change the code to work with zipWithIndex
    /*
    var i = 0;
    for ( seed <- seeds ) {
      System.setProperty("akka.cluster.seed-nodes.%d".format(i),"akka.tcp://%s@%s".format(clusterName,seed))
      i += 1
    }
     */
    // add roles to configuration
    var confTemp =
      ConfigFactory
        .parseString("akka.remote.netty.tcp.port=%d".format(port))
        .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=%s".format(hostname)))

    roles.foreach(
      rs =>
        confTemp =
          confTemp.withFallback(ConfigFactory.parseString("akka.cluster.roles = [%s]".format(rs.mkString(","))))
    )

    system = ActorSystem(
      clusterName,
      confTemp.withFallback(config.getConfig("cmwell.grid").withOnlyPath("akka").withFallback(config))
    )

    cluster = Cluster(system)
    val s = for (seed <- seeds) yield {
      val d = seed.split(":")
      val host: String = d(0)
      val port = d(1).toInt
      Address("akka.tcp", clusterName, host, port)
    }

    cluster.joinSeedNodes(s.toIndexedSeq)

    system.actorOf(Props[NodeActor], name = "clusterListener")
    Grid.create(classOf[ClientActor], ClientActor.name)
    Grid.create(classOf[LocalRegistrationManager], LocalRegistrationManager.name)
    val monitorActor = system.actorOf(Props[MonitorActor], name = MonitorActor.name)
//    IO(Http)(Grid.system) ! Http.Bind(monitorActor, interface = hostname, port = monitorPort)
    // ----------------------------------------------------------------------

    DMaps.get.foreach(_.initMaster)
    GridMonitor.init
    // ----------------------------------------------------------------------

    // first lets create the local settings actor
//    val settingsActor = system.actorOf(Props[SettingsActor],"settings")
//    settings = new Settings(settingsActor)

    // create a new internal actor
    internalActor = system.actorOf(Props[InternalActor], name = "internalactor")
    clusterSystemName = clusterName
    ServiceCoordinator.init
    RegistrationCoordinator.init
    registerServices(singletons)
  }

  def createAnon(props: Props): ActorRef = {
    val ref = system.actorOf(props)
    ref
  }

  def create(props: Props, name: String): ActorRef = {
    val ref = system.actorOf(props, name)
    addChild(name)
    ref
  }

  def createAnon(clazz: Class[_ <: Actor], args: Any*): ActorRef = {
    val p = Props(clazz, args: _*)
    val ref = system.actorOf(p)
    ref
  }

  def create(clazz: Class[_ <: Actor], name: String, args: Any*): ActorRef = {
    val p = Props(clazz, args: _*)
    val ref = system.actorOf(p, name)
    addChild(name)
    ref
  }

  def createAnon(clazz: Class[_ <: Actor]): ActorRef = {
    val p = Props(clazz)
    val ref = system.actorOf(p)
    ref
  }

  def create(clazz: Class[_ <: Actor], name: String): ActorRef = {
    val p = Props(clazz)
    val ref = system.actorOf(p, name)
    addChild(name)
    ref
  }

  def createOnJvm(jvm: GridJvm, clazz: Class[_ <: Actor], name: String, args: Any*): ActorRef = {
    val address = Address("akka.tcp", clusterSystemName, jvm.host, jvm.identity.get.systemPort)
    system.actorOf(Props(clazz, args: _*).withDeploy(Deploy(scope = RemoteScope(address))), name)
  }

  def createSomewhere(white: Set[GridJvm],
                      black: Set[GridJvm],
                      clazz: Class[_ <: Actor],
                      name: String,
                      args: Any*): ActorRef = {
    import scala.util.Random
    val jvms = Grid.jvmsAll

    var candidates = jvms

    if (!white.isEmpty) candidates = candidates.filter(white)
    if (!black.isEmpty) candidates = candidates.filterNot(black)

    val size = candidates.size

    val winnerId = Random.nextInt(size)
    val winner = candidates.toSeq(winnerId)

    createOnJvm(winner, clazz, name, args)
  }

  def createSingleton(clazz: Class[_ <: Actor], name: String, role: Option[String], args: Any*): ActorRef = {
    singletons = singletons.updated(name, role)
    val _settings = ClusterSingletonManagerSettings(system).withRole(role)
    val _settingsProxy = ClusterSingletonProxySettings(system).withRole(role)

    val _props = {
      if (args.isEmpty) Props(clazz)
      else Props(clazz, args: _*)
    }
    val p = ClusterSingletonManager.props(singletonProps = _props,
                                          terminationMessage = PoisonPill.getInstance,
                                          settings = _settings)

    val ref = system.actorOf(p, name)

    singletonsProxies = singletonsProxies + name
    system.actorOf(ClusterSingletonProxy.props(singletonManagerPath = s"/user/$name", settings = _settingsProxy),
                   name = s"${name}Proxy")

    ref
  }

  def getSingletons: Map[String, Option[String]] = {
    LocalServiceManager.mapping.map(
      tpl => tpl._1 -> tpl._2.map(gj => s"${gj.identity.map(_.name).getOrElse("")}@${gj.host}")
    )
  }

  def registerServices(st: ServiceTypes): Unit = {
    LocalServiceManager.init(st)
  }

  def serviceRef(name: String): ActorRef = {
    LocalServiceManager.serviceRef(name)
  }

  def selectSingleton(name: String, role: Option[String], proxy: GridJvm): ActorSelection = {
    val proxyName = s"${name}Proxy"
    if (!singletonsProxies.contains(name)) {
      singletonsProxies = singletonsProxies + name

      // following changes is just a patch for gitlab#167
      // without delving too deep (and fully understanding) what this code is for.
      // a thorough review (deep dive) is required here. (delete this comment when code is reviewed)
      lazy val _settings = ClusterSingletonProxySettings(system).withRole(role)

      val aRefFut = system.actorSelection(s"/user/$proxyName").resolveOne().andThen {
        case Failure(_) =>
          system.actorOf(
            ClusterSingletonProxy.props(singletonManagerPath = s"/user/$name", settings = _settings),
            name = proxyName
          )
      }

      //only waiting for side effect to take place
      Try(Await.ready(aRefFut, timeout.duration))
    }
    selectActor(proxyName, proxy)
  }

  def create_remote(clazz: Class[_ <: Actor], name: String, hostname: String, port: Int, args: Any*): ActorRef = {
    val p = Props(clazz, args: _*)
    val address = Address("akka.tcp", clusterSystemName, hostname, port)
    val remote = RemoteScope(address)
    val ref = system.actorOf(p.withDeploy(Deploy(scope = remote)), name)
    ref
  }

//  def create_remote( clazz: Class[_ <: Actor] , name : String , hostname : String , port : Int ) : ActorRef = {
//    val p = Props(clazz)
//    val address = Address("akka.tcp", clusterSystemName, hostname , port)
//    val remote = RemoteScope(address)
//    val ref = system.actorOf(p.withDeploy( Deploy(scope  = remote)), name)
//    ref
//  }

  def selectByPath(path: String): ActorSelection = {
    system.actorSelection(path)
  }

  def selectActorAnywhere(name: String): ActorSelection = {
    val path = s"akka.tcp://$clusterSystemName/*/user/$name"
    logger.trace(s"Selecting actor at: $path")
    system.actorSelection(path)
  }

  def selectActor(name: String, member: GridJvm): ActorSelection = {
    val path = s"akka.tcp://$clusterSystemName@${member.hostname}/user/$name"
    logger.trace(s"Selecting actor at: $path")
    system.actorSelection(path)
  }

  def shutdown: Unit = {
    val f = system.terminate()
    Await.result(f, Duration.Inf)
  }

  def terminate: Future[Unit] = system.terminate().map(_ => ())(scala.concurrent.ExecutionContext.Implicits.global)

  def run(clazz: Class[_ <: Actor], name: String, role: String): ActorRef = {
    val members = this.rolesMembers(role)
    // lets check if members empty

    // get random member with a role
    val member = members(scala.util.Random.nextInt(members.size))

    val actorRef = create_remote(clazz, name, member.host.get, member.port.get)
    actorRef
  }

  def actors(name: String, role: Option[String] = None): Vector[ActorSelection] = {
    val members: Vector[Address] = role match {
      case Some(role) =>
        this.rolesMembers(role)
      case None =>
        this.members.toVector
    }
    // now lets iterate all members and create actor vector
    //val members = Vector("172.17.0.4", "172.17.0.3", "172.17.0.2").map(ip => s"akka.tcp://cm-well-peDocker@$ip:7777")
    val vecActor = members.map { m =>
      val a = system.actorSelection(m.toString + "/user/" + name)
      a
    }
    vecActor
  }

  override def gridName: String = clusterSystemName

  def subscribeForGridEvents(subscriber: ActorRef) = {
    Grid.selectActor(ClientActor.name, Grid.thisMember) ! EventSubscription(subscriber)
  }
}
