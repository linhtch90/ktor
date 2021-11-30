/*
 * Copyright 2014-2019 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.server.netty.http1

import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.utils.io.*
import io.netty.buffer.*
import io.netty.channel.*
import io.netty.handler.codec.http.*
import kotlin.coroutines.*

internal class NettyHttp1ApplicationCall(
    application: Application,
    context: ChannelHandlerContext,
    httpRequest: HttpRequest,
    requestBodyChannel: ByteReadChannel?,
    engineContext: CoroutineContext,
    userContext: CoroutineContext
) : NettyApplicationCall(application, context, httpRequest) {

    override val request = NettyHttp1ApplicationRequest(
        this,
        engineContext,
        context,
        httpRequest,
        requestBodyChannel ?: ByteReadChannel.Empty
    )

    override val response = NettyHttp1ApplicationResponse(
        this,
        context,
        engineContext,
        userContext,
        httpRequest.protocolVersion()
    )

    init {
        putResponseAttribute()
    }

    override fun transform(buf: ByteBuf, last: Boolean): Any {
        if (isRaw) {
            return super.transform(buf, last)
        }
        return DefaultHttpContent(buf)
    }

    override fun endOfStream(lastTransformed: Boolean): Any? {
        if (isRaw) {
            return super.endOfStream(lastTransformed)
        }
        return LastHttpContent.EMPTY_LAST_CONTENT
    }

    override fun upgrade(dst: ChannelHandlerContext) {
        if (isRaw) {
            return super.upgrade(dst)
        }
        dst.pipeline().apply {
            replace(HttpServerCodec::class.java, "direct-encoder", NettyDirectEncoder())
        }
    }

    override fun isContextCloseRequired(): Boolean = !isRaw
}
