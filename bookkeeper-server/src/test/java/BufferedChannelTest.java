import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.Ignore;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BufferedChannelTest {
    @ParameterizedTest
    @MethodSource("provideAddArguments")
    @Timeout(value = 5)
    void testBufferedChannel(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity, long unpersistedBytesBound, boolean throwsException, Class<?> exceptionClass) throws IOException {
        try {
            BufferedChannel bc = new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
        } catch (Exception e) {
            assertTrue(throwsException);
            if (throwsException) assertEquals(exceptionClass, e.getClass());
            else throw e;
        }
    }

    static Stream<Arguments> provideAddArguments() throws IOException {
        return Stream.of(
                Arguments.of(null, getFullValidFileChannel("src/test/resources/test.txt"), 1, 1, 1, true, NullPointerException.class)
//                Arguments.of(getValidAllocator(), null, 1, 1, 1, true, NullPointerException.class),
//                Arguments.of(getValidAllocator(), getFullValidFileChannel("src/test/resources/test.txt"), 1, 1, 1, false, null),
//                Arguments.of(getValidAllocator(), getInvalidFileChannel(), 1, 1, 1, true, IOException.class)
        );
    }

    private static ByteBufAllocator getValidAllocator() {
        return UnpooledByteBufAllocator.DEFAULT;
    }

    public static FileChannel getFullValidFileChannel(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    public static FileChannel getInvalidFileChannel() throws IOException {
        FileChannel fileChannel = mock(FileChannel.class);

        when(fileChannel.isOpen()).thenReturn(false);

        return fileChannel;
    }


    private static ByteBufAllocator getValidAllocator1() {
        ByteBufAllocator allocator = mock(ByteBufAllocator.class);

        when(allocator.directBuffer(anyInt())).thenAnswer(invocationOnMock -> {
            System.out.println("ByteBufAllocator.allocator");

            int capacity = invocationOnMock.getArgument(0);
            ByteBuf buf = mock(ByteBuf.class);
            AtomicInteger writableBytes = new AtomicInteger(capacity);

            when(buf.writableBytes()).thenAnswer(invocation -> {
                System.out.println("ByteBuf.writableBytes: " + writableBytes.get());
                return writableBytes.get();
            });
            when(buf.readableBytes()).thenAnswer(invocation -> {
                System.out.println("ByteBuf.readableBytes: " + invocation.getArgument(0));
                return capacity - writableBytes.get();
            });

            when(buf.writeBytes(any(byte[].class), anyInt(), anyInt())).thenAnswer(invocation -> {
                System.out.println("ByteBuf.writeBytes: " + invocation.getArgument(0));

                int length = invocation.getArgument(2);

                if (length > writableBytes.get()) {
                    throw new IndexOutOfBoundsException("Not enough writable bytes");
                }

                writableBytes.addAndGet(-length);
                return buf;
            });

            when(buf.internalNioBuffer(anyInt(), anyInt())).thenAnswer(invocation -> {
                int index = invocation.getArgument(0);
                int length = invocation.getArgument(1);

                if (index < 0 || length < 0 || index + length > capacity) {
                    throw new IndexOutOfBoundsException("Index or length out of bounds");
                }

                ByteBuffer nioBuffer = ByteBuffer.allocate(length);
                System.out.println("ByteBuf.internalNioBuffer: index=" + index + ", length=" + length);
                return nioBuffer;
            });

            return buf;
        });

        return allocator;
    }

    private static ByteBufAllocator getInvalidAllocator1() {
        ByteBuf buf = mock(ByteBuf.class);
        when(buf.writableBytes()).thenThrow(new RuntimeException("Invalid ByteBuf"));
        when(buf.writeBytes(any(byte[].class), anyInt(), anyInt())).thenThrow(new RuntimeException("Invalid ByteBuf"));

        ByteBufAllocator allocator = mock(ByteBufAllocator.class);
        when(allocator.directBuffer(anyInt())).thenAnswer(invocationOnMock -> buf);

        return allocator;
    }

    private static FileChannel getValidFileChannel1() {
        FileChannel fileChannel = mock(FileChannel.class);

        try {
            // Configurazione di base per il mock
            when(fileChannel.read(any(ByteBuffer.class))).thenAnswer(invocation -> {
                ByteBuffer buffer = invocation.getArgument(0);
                int bytesRead = buffer.remaining(); // Simula lettura riempiendo il buffer
                buffer.put(new byte[bytesRead]);
                return bytesRead;
            });

            when(fileChannel.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
                ByteBuffer buffer = invocation.getArgument(0);
                int bytesWritten = buffer.remaining();
                buffer.position(buffer.limit()); // Simula la scrittura aggiornando la posizione del buffer
                return bytesWritten;
            });

            when(fileChannel.size()).thenReturn(1024L); // Simula un file di 1024 byte

            when(fileChannel.position(anyLong())).thenAnswer(invocation -> {
                long newPosition = invocation.getArgument(0);
                return fileChannel;
            });

            when(fileChannel.position()).thenReturn(0L); // Simula la posizione iniziale a 0

            when(fileChannel.isOpen()).thenReturn(true);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return fileChannel;
    }

    private static FileChannel getInvalidFileChannel1() {
        return null;
    }
}
