/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.operation

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TGetOperationStatusResp, TOperationState, TProtocolVersion}
import org.apache.hive.service.rpc.thrift.TOperationState._

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.events.{EventBus, KyuubiOperationEvent}
import org.apache.kyuubi.operation.ExecuteStatement.DEFAULT_SAME_STATE_UPDATE_INTERVAL
import org.apache.kyuubi.operation.FetchOrientation.FETCH_NEXT
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, Session}

class ExecuteStatement(
    session: Session,
    override val statement: String,
    confOverlay: Map[String, String],
    override val shouldRunAsync: Boolean,
    queryTimeout: Long)
  extends KyuubiOperation(session) {
  EventBus.post(KyuubiOperationEvent(this))

  final private val _operationLog: OperationLog =
    if (shouldRunAsync) {
      OperationLog.createOperationLog(session, getHandle)
    } else {
      null
    }

  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  override def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(true)
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  private def executeStatement(): Unit = {
    try {
      // We need to avoid executing query in sync mode, because there is no heartbeat mechanism
      // in thrift protocol, in sync mode, we cannot distinguish between long-run query and
      // engine crash without response before socket read timeout.
      _remoteOpHandle = client.executeStatement(statement, confOverlay, true, queryTimeout)
      setHasResultSet(_remoteOpHandle.isHasResultSet)
    } catch onError()
  }

  private def waitStatementComplete(): Unit =
    try {
      setState(OperationState.RUNNING)
      var statusResp: TGetOperationStatusResp = null

      // initialize operation status
      while (statusResp == null) {
        statusResp = client.getOperationStatus(_remoteOpHandle)
      }

      var isComplete = false
      var lastState: TOperationState = null
      var lastStateUpdateTime: Long = 0L
      while (!isComplete) {
        fetchQueryLog()
        verifyTStatus(statusResp.getStatus)
        if (statusResp.getProgressUpdateResponse != null) {
          setOperationJobProgress(statusResp.getProgressUpdateResponse)
        }
        val remoteState = statusResp.getOperationState
        if (lastState != remoteState ||
          System.currentTimeMillis() - lastStateUpdateTime > DEFAULT_SAME_STATE_UPDATE_INTERVAL) {
          lastStateUpdateTime = System.currentTimeMillis()
          info(s"Query[$statementId] in ${remoteState.name()}")
        }
        lastState = remoteState
        isComplete = true
        remoteState match {
          case INITIALIZED_STATE | PENDING_STATE | RUNNING_STATE =>
            isComplete = false
            statusResp = client.getOperationStatus(_remoteOpHandle)

          case FINISHED_STATE =>
            setState(OperationState.FINISHED)

          case CLOSED_STATE =>
            setState(OperationState.CLOSED)

          case CANCELED_STATE =>
            setState(OperationState.CANCELED)

          case TIMEDOUT_STATE
              // Clients less than version 2.1 have no HIVE-4924 Patch,
              // no queryTimeout parameter and no TIMEOUT status.
              // When the server enables kyuubi.operation.query.timeout,
              // this will cause the client of the lower version to get stuck.
              // Check thrift protocol version <= HIVE_CLI_SERVICE_PROTOCOL_V8(Hive 2.1.0),
              // convert TIMEDOUT_STATE to CANCELED.
              if getProtocolVersion.getValue <=
                TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8.getValue =>
            setState(OperationState.CANCELED)

          case TIMEDOUT_STATE =>
            setState(OperationState.TIMEOUT)

          case ERROR_STATE =>
            throw KyuubiSQLException(statusResp.getErrorMessage)

          case UKNOWN_STATE =>
            throw KyuubiSQLException(s"UNKNOWN STATE for $statement")
        }
        sendCredentialsIfNeeded()
      }
      // see if anymore log could be fetched
      fetchQueryLog()
    } catch onError()

  private def sendCredentialsIfNeeded(): Unit = {
    val appUser = session.asInstanceOf[KyuubiSessionImpl].engine.appUser
    val sessionManager = session.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.credentialsManager.sendCredentialsIfNeeded(
      session.handle.identifier.toString,
      appUser,
      client.sendCredentials)
  }

  private def fetchQueryLog(): Unit = {
    getOperationLog.foreach { logger =>
      try {
        val ret = client.fetchResults(_remoteOpHandle, FETCH_NEXT, 1000, fetchLog = true)
        val logs = ret.getColumns.get(0).getStringVal.getValues.asScala
        logs.foreach(log => logger.write(log + "\n"))
      } catch {
        case _: Exception => // do nothing
      }
    }
  }

  override protected def runInternal(): Unit = {
    executeStatement()
    val sessionManager = session.sessionManager
    val asyncOperation: Runnable = () => waitStatementComplete()
    try {
      val opHandle = sessionManager.submitBackgroundOperation(asyncOperation)
      setBackgroundHandle(opHandle)
    } catch onError("submitting query in background, query rejected")

    if (!shouldRunAsync) getBackgroundHandle.get()
  }

  override def setState(newState: OperationState): Unit = {
    super.setState(newState)
    EventBus.post(KyuubiOperationEvent(this))
  }
}

object ExecuteStatement {
  final val DEFAULT_SAME_STATE_UPDATE_INTERVAL = 5000L
}
