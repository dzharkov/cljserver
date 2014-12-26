package ru.spbau.mit.cljserver

import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.net.InetSocketAddress
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import java.nio.ByteBuffer
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.log4j.Logger
import mikera.cljutils.Clojure

fun main(args: Array<String>) {
    val selector: Selector = Selector.open() 
    val server: ServerSocketChannel = ServerSocketChannel.open()
    server.socket().bind(InetSocketAddress(2007))
    server.configureBlocking(false)
    server.register(selector, SelectionKey.OP_ACCEPT)

    val readingBuffers = hashMapOf<SocketChannel, ByteBuffer>()
    val writingBuffers = hashMapOf<SocketChannel, ByteBuffer>()

    val executorService = Executors.newFixedThreadPool(4)
    val taskResults = ConcurrentLinkedQueue<TaskResult>()
    
    var maxActiveClients = 0

    Runtime.getRuntime().addShutdownHook(
        object : Thread() {
            override fun run() {
                println("maxActiveClients = $maxActiveClients")
            }
        }
    )
    
    while (true) {
        selector.select()
        val keysIterator: MutableIterator<SelectionKey> = selector.selectedKeys().iterator()
        keysIterator.forEachRemoving { key ->
            if (key.isAcceptable()) {
                val client = server.accept()
                client.configureBlocking(false)
                client.socket().setTcpNoDelay(true)
                client.register(
                        selector, 
                        SelectionKey.OP_READ or SelectionKey.OP_CONNECT
                )

                writingBuffers[client] = ByteBuffer.allocate(200000).limit(0) as ByteBuffer
                readingBuffers[client] = ByteBuffer.allocate(200000).limit(4) as ByteBuffer
                maxActiveClients = Math.max(maxActiveClients, readingBuffers.size())
            } else {
                val channel = key.socketChannel()
                when {
                    key.isReadable() -> {
                        val byteBuffer = readingBuffers[channel]

                        val size = try { channel.read(byteBuffer) } catch (e: Throwable) { -1 }
                        if (size == -1) {
                            channel.finishConnect()
                            channel.close()
                            key.cancel()

                            writingBuffers.remove(channel)
                            readingBuffers.remove(channel)
                        }
                        
                        if (size > 0) {
                            if (byteBuffer.limit() == 4 && !byteBuffer.hasRemaining()) {
                                byteBuffer.limit(4 + byteBuffer.getInt(0))
                            }

                            if (!byteBuffer.hasRemaining()) {
                                val request = String(byteBuffer.array(), 4, byteBuffer.position() - 4)
                                executorService.execute {
                                    taskResults.add(TaskResult(channel, runTask(request)))
                                    selector.wakeup()
                                }
                                byteBuffer.clear()
                                byteBuffer.limit(4)
                            }
                        }
                    }
                    key.isWritable() -> {
                        val buffer = writingBuffers[channel]
                        if (buffer.hasRemaining()) {
                            channel.write(buffer)
                            if (!buffer.hasRemaining()) {
                                channel.register(selector, SelectionKey.OP_READ or SelectionKey.OP_CONNECT)
                            }
                        }
                    }
                    else -> {
                        throw AssertionError(key.toString())                       
                    }
                }
            }
        }

        while (taskResults.isNotEmpty()) {
            val result = taskResults.poll()
            val byteBuffer = writingBuffers[result.channel] ?: continue
            byteBuffer.clear()
            byteBuffer.putInt(result.response.length())
            byteBuffer.put(result.response.toByteArray())
            byteBuffer.limit(byteBuffer.position())
            byteBuffer.position(0)
            
            result.channel.register(selector, SelectionKey.OP_WRITE or SelectionKey.OP_CONNECT)
        }
    }
}

fun runTask(request: String): String = try {
    Clojure.eval(request).toString()
} catch (t: Throwable) {
    "Error: ${t.toString()}"
}

data class TaskResult(val channel: SocketChannel, val response: String)

inline fun <T> MutableIterator<T>.forEachRemoving(block: (T) -> Unit) {
    while (hasNext()) {
        val next = next()
        remove()
        block(next)
    }
}

fun SelectionKey.socketChannel(): SocketChannel = channel() as SocketChannel