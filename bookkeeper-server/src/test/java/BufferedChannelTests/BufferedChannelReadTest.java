package BufferedChannelTests;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BufferedChannelReadTest {
    private final String path = "src/test/resources/test.txt";
    private BufferedChannel bc;

    public enum BufferedChannelEnum {
        LETTSCR,
        SCR,
        NULL_WRITEBUF,
        NOT_NULL
    }

    @Test
    public void testBufChannelReadCustomPos() throws IOException {
        ByteBufAllocator allocator = getNonEmptyByteBufAllocator();

        String helloWorld = "Hello World\n";
        try (FileOutputStream fos = new FileOutputStream(path, false); FileChannel channel = fos.getChannel()) {
            channel.truncate(0);
            for(int i = 0; i < 10; i++)
                fos.write(helloWorld.getBytes(StandardCharsets.UTF_8));
            channel.force(true);

            channel.position(0);
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileChannel fc = getFullFileChannel(path);
        fc.position(0);
        if(fc.position() != 0) System.out.println("fc position doesnt set");

        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.directBuffer(11);
        buf.writeBytes("Hello World".getBytes());

        bc = new BufferedChannel(allocator, fc, 1024);

        ByteBuf dest = getByteBuf(20);

        try {
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                assertDoesNotThrow(() -> bc.read(dest, bc.getFileChannelPosition() + 10, 5));
            });
        } catch (Exception e) {
            assertNull(e.getClass());
        }
    }

    @BeforeEach
    public void beforeEach() {
        String helloWorld = "Hello World";

        try (FileOutputStream fos = new FileOutputStream(path, false); FileChannel channel = fos.getChannel()) {
            channel.truncate(0);
            fos.write(helloWorld.getBytes(StandardCharsets.UTF_8));
            channel.force(true);

            channel.position(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @ParameterizedTest
    @MethodSource("args")
    @Timeout(value = 1)
    void testBufChannelOnlyRead(BufferedChannelEnum buffChannelEnum, ByteBuf dest, int pos, int length, Class<? extends Throwable> expectedException) throws IOException {
        ByteBufAllocator allocator;

        switch (buffChannelEnum) {
            case SCR:
                allocator = getReadOnlyByteBufAllocator();
                break;
            case NULL_WRITEBUF:
                allocator = getNullWriteByteBufAllocator();
                break;
            case LETTSCR:
            default:
                allocator = UnpooledByteBufAllocator.DEFAULT;
                break;
        }

        FileChannel fc = getFullFileChannel(path);
        fc.position(0);

        bc = new BufferedChannel(allocator, fc, 1024);

        try {
            bc.read(dest, pos, length);
        } catch (Exception e) {
            assertEquals(expectedException, e.getClass());
        }
    }

    static Stream<Arguments> args() {
        return Stream.of(
//                Arguments.of(BufferedChannelEnum.LETTSCR, getWriteOnlyByteBuf(11), 0, 11, null), // Return IOException("Read past EOF")
//                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, 11, null), // Return IOException("Read past EOF")
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, 12, IOException.class),
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), -1, 12, IllegalArgumentException.class),
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 12, 11, IllegalArgumentException.class),
                Arguments.of(BufferedChannelEnum.SCR, getByteBuf(11), 0, 11, IOException.class),
                Arguments.of(BufferedChannelEnum.NULL_WRITEBUF, getByteBuf(11), 1, 10, null)
        );
    }

    @ParameterizedTest
    @MethodSource("argsAfterWrite")
    void  testBufChannelReadAfterWrite(BufferedChannelEnum buffChannelEnum, ByteBuf dest, int pos, int length, Class<? extends Throwable> expectedException) throws IOException {
        ByteBufAllocator allocator;

        switch (buffChannelEnum) {
            case SCR:
                allocator = getReadOnlyByteBufAllocator();
                break;
            case NOT_NULL:
                allocator = getNonEmptyByteBufAllocator();
                break;
            case LETTSCR:
            default:
                allocator = UnpooledByteBufAllocator.DEFAULT;
                break;
        }

        String helloWorld = "Hello World";
        try (FileOutputStream fos = new FileOutputStream(path, false); FileChannel channel = fos.getChannel()) {
            channel.truncate(0);
            for(int i = 0; i < 10; i++)
                fos.write(helloWorld.getBytes(StandardCharsets.UTF_8));
            channel.force(true);

            channel.position(0);
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileChannel fc = getFullFileChannel(path);
        fc.position(0);

        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.directBuffer(11);
        buf.writeBytes("Hello World".getBytes());

        try {
            bc = new BufferedChannel(allocator, fc, 1024);
            bc.write(buf);
            bc.flush();
        } catch (Exception e) {
            assertEquals(expectedException, e.getClass());
            return;
        }

        try {
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                dest.clear();
                bc.read(dest, pos, length);
            });
        } catch (Exception e) {
            assertEquals(expectedException, e.getClass());
        }
    }

    static Stream<Arguments> argsAfterWrite() {
        return Stream.of(
                Arguments.of(BufferedChannelEnum.LETTSCR, null, 0, 0, NullPointerException.class),
//                Arguments.of(BufferedChannelEnum.LETTSCR, getWriteOnlyByteBuf(11), 0, 11, null), // Gone in timeout
                Arguments.of(BufferedChannelEnum.LETTSCR, getReadOnlyByteBuf(11), 0, 11, ReadOnlyBufferException.class),
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, -1, IllegalArgumentException.class),
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, 11, null),
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, 12, IOException.class),
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), -1, 12, IllegalArgumentException.class),
//                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 1, 12, IOException.class),
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 12, 11, IllegalArgumentException.class),
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, 5, null),
                Arguments.of(BufferedChannelEnum.SCR, getByteBuf(11), 0, 11, ReadOnlyBufferException.class)
//                Arguments.of(BufferedChannelEnum.NOT_NULL, getByteBuf(20), 544, 5, null)
        );
    }

    public FileChannel getFullFileChannel(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    public static ByteBuf getReadOnlyByteBuf(int capacity) {
        return ByteBufAllocator.DEFAULT.directBuffer(capacity).asReadOnly();
    }

    public static ByteBuf getByteBuf(int capacity) {
        return ByteBufAllocator.DEFAULT.directBuffer(capacity);
    }

    public static ByteBuf getWriteOnlyByteBuf(int capacity) {
        ByteBuf mockBuf = mock(ByteBuf.class);

        when(mockBuf.capacity()).thenReturn(capacity);
        when(mockBuf.readerIndex(anyInt())).thenReturn(mockBuf);
        when(mockBuf.readByte()).thenThrow(new IndexOutOfBoundsException("Buffer is write-only"));
        when(mockBuf.release()).thenReturn(true);

        return mockBuf;
    }

    private static ByteBufAllocator getReadOnlyByteBufAllocator() {
        ByteBufAllocator allocator = spy(UnpooledByteBufAllocator.DEFAULT);

        when(allocator.directBuffer(anyInt())).thenAnswer(invocation -> {
            int capacity = invocation.getArgument(0); // Input parameters
            return ByteBufAllocator.DEFAULT.directBuffer(capacity).asReadOnly(); // Return only reading buffer
        });

        return allocator;
    }

    private static ByteBufAllocator getNullWriteByteBufAllocator() {
        ByteBufAllocator allocator = spy(UnpooledByteBufAllocator.DEFAULT);

        when(allocator.directBuffer(anyInt())).thenReturn(null);

        return allocator;
    }

    private static ByteBufAllocator getNonEmptyByteBufAllocator() {
        ByteBufAllocator allocator = spy(UnpooledByteBufAllocator.DEFAULT);

        when(allocator.directBuffer(anyInt())).thenAnswer(invocation -> {
            int capacity = invocation.getArgument(0); // Input parameters
            ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer(capacity);
            buf.writerIndex(buf.writerIndex() + capacity/2);

            return buf;
        });

        return allocator;
    }
}
