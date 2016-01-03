package com.example

import akka.actor.{PersistentActor, Props, Success => AkkaSuccess, Failure => AkkaFailure}
/*
This actor provides a way to keep track of a particular
city. It allows the user to set the tempurature, and apply
deltas
*/

object TempuratureActor {

   /**
    * Case classes in this sample, but in prod we use
    * Thrift strcuts to more easily reason about serializaion
    * guaruntees. Pro-tip.
    */

    sealed trait TempuratureCmd
    case class IncreaseTempurature(delta: Int) extends TempuratureCmd
    case class DecreaseTempurature(delta: Int) extends TempuratureCmd
    case class SetTempurature(tempInCelsius: Int) extends TempuratureCmd
    case object GetTempurature

    // Only used to take snapshots
    private case class TempuratureSnapshot(temp: Int)

    case class TempuratureException(override val getMessage: String)
        extends RuntimeException

    case object NoInitialTempuratureException
        extends RuntimeException("There was not initial state. Cannot apply a delta.")


    def props(city: String, id: String): Props = {
        Props(classOf[TempuratureActor], city, id)
    }
}


class TempuratureActor(city: String, id: String) extends PersistentActor {
    import TempuratureActor._

    // How many times to journal before taking a snapshot
    private final val SnapshotInterval = 500

    private var consecutivePersists = 0

    var currentTempurature: Option[Int] = None


    override val persistenceId: String = s"$city-$id-temp"


    override def receiveRecover: Receive = {
        case SnapshotOffer(_, TempuratureSnapshot(t)) =>
            this.currentTempurature = Some(t)

        case cmd :TempuratureCmd =>
            updateState(cmd)
    }


    override def receiveCommand: Receive = {
        case GetTempurautre =>
            this.currentTempurautre match {
                case Some(t) =>
                    sender() ! t

                case _ =>
                    sender ! AkkaFalilure(new TempuratureException("No tempurature to get"))
            }

        case cmd: TempuratureCmd =>
            persist(cmd){ persistedCmd =>
                incrementPersistCount()
                if (shouldSnapshot()) {
                    this.currentTempurature foreach { t =>
                        snapshotAndSweep(TempuratureSnapshot(t))
                    }
                }

                updateState(persistedCmd) match {
                    case Success(_) =>
                        sender() ! AkkaSuccess(())

                    case Failure(err) =>
                        sender() ! AkkaFailure(err)
                }
            }
    }


    private def updateState(cmd: TempuratureCmd): Try[Unit] = Try {
        cmd match {
            case SetTempurature(t) =>
                this.currentTempurature = Some(t)
                ()

            case IncreaseTempurature(delta) if hasTempuratureState =>
                this.currentTempurature = this.currentTempurature map(_ + delta)
                ()

            case DecreaseTempurature(delta) if hasTempuratureState =>
                this.currentTempurature = this.currentTempurature map(_ - delta)
                ()

            case _ =>
                throw NoInitialTempuratureException
        }
    }


    /*
    Take a snapshot and delete journal messages
    older that it
    */
    private def snapshotAndSweep(snapshot: TempuratureSnapshot) = {
        saveSnapshot(snapshot)
        deleteMessages(snapshotId)
    }


    private def hasTempuratureState = currentTempurature.isDefined


    private def incrementPersistCount = {
        this.consecutivePersists = (this.consecutivePersists + 1) % SnapshotInterval
    }


    private def shouldSnapshot = {
        0 == this.consecutivePersists
    }
}
