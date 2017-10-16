package cmwell.util.stream

import akka.stream.{AmorphousShape, Attributes, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.concurrent.{ExecutionContext, Future}
import scala.util._



/**
  * Proj: server
  * User: gilad
  * Date: 9/4/17
  * Time: 1:20 PM
  */
class GlobalVarStateHandler[State](setters: Int, getters: Int)(initialAsync: () ⇒ Future[State])(implicit ec: ExecutionContext) extends GraphStage[SingleTypeAmorphousShape[State,State]] {
  require(setters >= 0 && getters >= 0 && setters + getters > 0, "must have positive number of either inlets or outlets")

  val inlets: List[Inlet[State]] = List.tabulate(setters)(i => Inlet[State]("GlobalVarStateHandler.in" + i))
  val outlets: List[Outlet[State]] = List.tabulate(getters)(i => Outlet[State]("GlobalVarStateHandler.in" + i))

  override val shape: SingleTypeAmorphousShape[State,State] = SingleTypeAmorphousShape(inlets,outlets)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var state: State = null.asInstanceOf[State]
    var hasActiveSetters: Boolean = setters > 0

    outlets.foreach { out =>
      setHandler(out, new OutHandler {
        override def onDownstreamFinish(): Unit = {
          if(inlets.forall(isClosed[State]) && outlets.forall(isClosed[State]))
            completeStage()
        }
        override def onPull(): Unit = {
          if(state != null) push(out,state)
          // else when AsyncCallback returns with value,
          // or when inlets override initial value,
          // we will replace handlers and return state.
        }
      })
    }

    inlets.foreach { in =>
      setHandler(in, new InHandler {
        override def onUpstreamFinish(): Unit = {
          if(inlets.forall(isClosed[State])) {
            if (outlets.forall(isClosed[State])) {
              completeStage()
            } else hasActiveSetters = false
          }
        }
        override def onPush(): Unit = {
          state = grab(in)
          outlets.foreach { out =>
            if(isAvailable(out)) push(out,state)
          }
          pull(in)
        }
      })
    }

    def replaceHandlers: Unit = {
      outlets.foreach { out =>
        setHandler(out, new OutHandler {
          override def onDownstreamFinish(): Unit = {
            if(inlets.forall(isClosed[State]) && outlets.forall(isClosed[State]))
              completeStage()
          }
          override def onPull(): Unit = push(out,state)
        })
      }
      inlets.foreach { in =>
        setHandler(in, new InHandler {
          override def onUpstreamFinish(): Unit = {
            if(inlets.forall(isClosed[State]) && outlets.forall(isClosed[State]))
              completeStage()
          }
          override def onPush(): Unit = {
            state = grab(in)
            pull(in)
          }
        })
      }
    }

    override def preStart(): Unit = {

      val acb = getAsyncCallback[Try[State]] {
        case Failure(ex) if state != null && hasActiveSetters ⇒ replaceHandlers
        case Failure(ex) ⇒ failStage(ex)
        case Success(initialState) ⇒ {

          if(state == null)
            state = initialState

          outlets.foreach { out =>
            if(isAvailable(out)) push(out,state)
          }

          replaceHandlers
        }
      }

      initialAsync().onComplete(acb.invoke)
      inlets.foreach(tryPull)
    }
  }
}
