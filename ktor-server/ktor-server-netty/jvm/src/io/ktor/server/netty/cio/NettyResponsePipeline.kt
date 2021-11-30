/*
* Copyright 2014-2021 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
*/

package io.ktor.server.netty.cio

import io.ktor.http.*
import io.ktor.server.netty.*
import io.ktor.util.*
import io.ktor.util.cio.*
import io.ktor.utils.io.*
import io.netty.channel.*
import io.netty.handler.codec.http.*
import io.netty.handler.codec.http2.*
import kotlinx.coroutines.*
import java.io.*
import java.util.concurrent.atomic.*
import kotlin.coroutines.*

private const val UNFLUSHED_LIMIT = 65536

/**
 * Contains methods for handling http request with Netty
 * @param context
 * @param coroutineContext
 * @param activeRequests
 * @param isCurrentRequestFullyRead
 * @param isChannelReadComplete
 */
@OptIn(InternalAPI::class)
internal class NettyResponsePipeline constructor(
    private val context: ChannelHandlerContext,
    override val coroutineContext: CoroutineContext,
    private val activeRequests: AtomicLong,
    private val isCurrentRequestFullyRead: AtomicBoolean,
    private val isChannelReadComplete: AtomicBoolean
) : CoroutineScope {
    /**
     * True if there is unflushed written data in channel
     */
    private val isWriteOccurred: AtomicBoolean = AtomicBoolean(false)

    /**
     * Represents promise which is marked as success when the last read request is handled.
     * Marked as fail when last read request is failed.
     * Default value is success on purpose to start first request handle
     */
    private var previousCallHandled: ChannelPromise = context.newPromise().also {
        it.setSuccess()
    }

    /**
     * Flush if there is some unflushed data, nothing to read from channel and no active requests
     */
    internal fun flushIfNeeded() {
        if (isWriteOccurred.get() && isChannelReadComplete.get() && activeRequests.get() == 0L) {
            context.flush()
            isWriteOccurred.set(false)
        }
    }

    internal fun processResponse(call: NettyApplicationCall) {
        call.previousCallFinished = previousCallHandled
        call.callFinished = context.newPromise()
        previousCallHandled = call.callFinished

        processElement(call)
    }

    private fun processElement(call: NettyApplicationCall) {
        val previousCallFinished = call.previousCallFinished

        if (previousCallFinished.isDone && !previousCallFinished.isSuccess) {
            processCallFailed(call, previousCallFinished.cause())
            return
        }

        call.response.responseFlag.addListener {
            if (!it.isSuccess) {
                processCallFailed(call, it.cause())
                return@addListener
            }

            call.previousCallFinished.addListener waitPreviousCall@{ previousCallResult ->
                if (!previousCallResult.isSuccess) {
                    processCallFailed(call, it.cause())
                    return@waitPreviousCall
                }
                try {
                    processCall(call)
                } catch (actualException: Throwable) {
                    processCallFailed(call, actualException)
                } finally {
                    call.responseWriteJob.cancel()
                }
            }
        }
    }

    private fun processCallFailed(call: NettyApplicationCall, actualException: Throwable) {
        val t = when {
            actualException is IOException && actualException !is ChannelIOException ->
                ChannelWriteException(exception = actualException)
            else -> actualException
        }

        call.response.responseChannel.cancel(t)
        call.responseWriteJob.cancel()
        call.response.cancel()
        call.dispose()
        call.callFinished.setFailure(t)
    }

    private fun processUpgrade(call: NettyApplicationCall, responseMessage: Any): ChannelFuture {
        val future = context.write(responseMessage)
        call.upgrade(context)
        call.isRaw = true

        context.flush()
        isWriteOccurred.set(false)
        return future
    }

    private fun finishCall(
        call: NettyApplicationCall,
        lastMessage: Any?,
        lastFuture: ChannelFuture
    ) {
        val prepareForClose =
            (!call.request.keepAlive || call.response.isUpgradeResponse()) && call.isContextCloseRequired()

        val lastMessageFuture = if (lastMessage != null) {
            val future = context.write(lastMessage)
            isWriteOccurred.set(true)
            future
        } else {
            null
        }

        activeRequests.decrementAndGet()
        call.callFinished.setSuccess()

        lastMessageFuture?.addListener {
            if (prepareForClose) {
                close(lastFuture)
                return@addListener
            }
        }
        if (prepareForClose) {
            close(lastFuture)
            return
        }
        scheduleFlush()
    }

    fun close(lastFuture: ChannelFuture) {
        context.flush()
        isWriteOccurred.set(false)
        lastFuture.addListener {
            context.close()
        }
    }

    private fun scheduleFlush() {
        context.executor().execute {
            flushIfNeeded()
        }
    }

    private fun processCall(call: NettyApplicationCall) {
        val responseMessage = call.response.responseMessage
        val response = call.response

        val requestMessageFuture = if (response.isUpgradeResponse()) {
            processUpgrade(call, responseMessage)
        } else {
            val currentWritersCount = activeRequests.get()
            if (isChannelReadComplete.get() &&
                (currentWritersCount == 0L ||
                    (!isCurrentRequestFullyRead.get() &&
                        currentWritersCount == 1L))
            ) {
                val future = context.writeAndFlush(responseMessage)
                isWriteOccurred.set(false)
                future
            } else {
                val future = context.write(responseMessage)
                isWriteOccurred.set(true)
                future
            }
        }

        if (responseMessage is FullHttpResponse) {
            return finishCall(call, null, requestMessageFuture)
        } else if (responseMessage is Http2HeadersFrame && responseMessage.isEndStream) {
            return finishCall(call, null, requestMessageFuture)
        }

        val responseChannel = response.responseChannel
        val bodySize = when {
            responseChannel === ByteReadChannel.Empty -> 0
            responseMessage is HttpResponse -> responseMessage.headers().getInt("Content-Length", -1)
            responseMessage is Http2HeadersFrame -> responseMessage.headers().getInt("content-length", -1)
            else -> -1
        }

        launch(context.executor().asCoroutineDispatcher(), start = CoroutineStart.UNDISPATCHED) {
            processResponseBody(
                call,
                response,
                bodySize,
                requestMessageFuture
            )
        }
    }

    private suspend fun processResponseBody(
        call: NettyApplicationCall,
        response: NettyApplicationResponse,
        bodySize: Int,
        requestMessageFuture: ChannelFuture
    ) {
        try {
            when (bodySize) {
                0 -> processEmpty(call, requestMessageFuture)
                in 1..65536 -> processSmallContent(call, response, bodySize)
                -1 -> processBodyFlusher(call, response, requestMessageFuture)
                else -> processBodyGeneral(call, response, requestMessageFuture)
            }
        } catch (actualException: Throwable) {
            processCallFailed(call, actualException)
        }
    }

    private fun processEmpty(call: NettyApplicationCall, lastFuture: ChannelFuture) {
        return finishCall(call, call.endOfStream(false), lastFuture)
    }

    private suspend fun processSmallContent(call: NettyApplicationCall, response: NettyApplicationResponse, size: Int) {
        val buffer = context.alloc().buffer(size)
        val channel = response.responseChannel
        val start = buffer.writerIndex()

        channel.readFully(buffer.nioBuffer(start, buffer.writableBytes()))
        buffer.writerIndex(start + size)

        val future = context.write(call.transform(buffer, true))
        isWriteOccurred.set(true)

        val lastMessage = response.trailerMessage() ?: call.endOfStream(true)

        finishCall(call, lastMessage, future)
    }

    private suspend fun processBodyGeneral(
        call: NettyApplicationCall,
        response: NettyApplicationResponse,
        requestMessageFuture: ChannelFuture
    ) = processBodyBase(call, response, requestMessageFuture) { _, unflushedBytes ->
        unflushedBytes >= UNFLUSHED_LIMIT
    }

    private suspend fun processBodyFlusher(
        call: NettyApplicationCall,
        response: NettyApplicationResponse,
        requestMessageFuture: ChannelFuture
    ) = processBodyBase(call, response, requestMessageFuture) { channel, unflushedBytes ->
        unflushedBytes >= UNFLUSHED_LIMIT || channel.availableForRead == 0
    }

    private suspend fun processBodyBase(
        call: NettyApplicationCall,
        response: NettyApplicationResponse,
        requestMessageFuture: ChannelFuture,
        flushCondition: (channel: ByteReadChannel, unflushedBytes: Int) -> Boolean
    ) {
        val channel = response.responseChannel

        var unflushedBytes = 0
        var lastFuture: ChannelFuture = requestMessageFuture

        @Suppress("DEPRECATION")
        channel.lookAheadSuspend {
            while (true) {
                val buffer = request(0, 1)
                if (buffer == null) {
                    if (!awaitAtLeast(1)) break
                    continue
                }

                val rc = buffer.remaining()
                val buf = context.alloc().buffer(rc)
                val idx = buf.writerIndex()
                buf.setBytes(idx, buffer)
                buf.writerIndex(idx + rc)

                consumed(rc)
                unflushedBytes += rc

                val message = call.transform(buf, false)

                if (flushCondition.invoke(channel, unflushedBytes)) {
                    context.read()
                    val future = context.writeAndFlush(message)
                    isWriteOccurred.set(false)
                    lastFuture = future
                    future.suspendAwait()
                    unflushedBytes = 0
                } else {
                    lastFuture = context.write(message)
                    isWriteOccurred.set(true)
                }
            }
        }

        val lastMessage = response.trailerMessage() ?: call.endOfStream(false)
        finishCall(call, lastMessage, lastFuture)
    }
}

@OptIn(InternalAPI::class)
private fun NettyApplicationResponse.isUpgradeResponse() =
    status()?.value == HttpStatusCode.SwitchingProtocols.value
