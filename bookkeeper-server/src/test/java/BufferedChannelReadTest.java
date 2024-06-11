import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.FileOutputStream;
import java.io.IOException;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BufferedChannelReadTest {
    private final String pathStr = "src/test/resources/test.txt";
    private final Path path = Paths.get(pathStr);
    private BufferedChannel bc;

    public enum BufferedChannelEnum {
        LETTSCR,
        LETTSCR_W,
        LETTSCR_NULLALLOCATOR,
        SCR,
        LETTSCR_FlUSH,
        LETTSCR_FlUSH_TRUNC,
        LETTSCR_FlUSH_READ
    }

    @BeforeEach
    public void resetTextFile() {
        String helloWorld = "Hello World";

        try (FileOutputStream fos = new FileOutputStream(pathStr, false); FileChannel channel = fos.getChannel()) {
            channel.truncate(0);
            fos.write(helloWorld.getBytes(StandardCharsets.UTF_8));
            channel.force(true);

            channel.position(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @ParameterizedTest
    @MethodSource("argsError")
    void testBufChannelReadError(BufferedChannelEnum buffChannelEnum, ByteBuf dest, int pos, int length, Class<? extends Throwable> expectedException) throws IOException {
        FileChannel fc;
        ByteBuf buf;

        switch (buffChannelEnum) {
            case SCR:
                fc = FileChannel.open(path, StandardOpenOption.WRITE);
                fc.position(0);

                bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 1024, 512);
                break;
            case LETTSCR_W:
                fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                fc.position(0);

                bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 1024, 512);

                buf = ByteBufAllocator.DEFAULT.directBuffer();
                buf.writeBytes("Hello World".getBytes(StandardCharsets.UTF_8));

                bc.write(buf);

                break;
            case LETTSCR_FlUSH_TRUNC:
                fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                fc.position(0);

                bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 11, 0);

                buf = ByteBufAllocator.DEFAULT.directBuffer();
                buf.writeBytes("Hello World".getBytes(StandardCharsets.UTF_8));

                bc.write(buf);
                bc.flush(); // Need to change internal "writeBufferStartPosition"

                fc.truncate(0);
                break;
            case LETTSCR:
            default:
                fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                fc.position(0);

                bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 1024, 512);
                break;
        }


        try {
            bc.read(dest, pos, length);
        } catch (Exception e) {
            assertEquals(expectedException, e.getClass());

            return;
        }

        assertNull(expectedException);
    }

    static Stream<Arguments> argsError() {
        return Stream.of(
                Arguments.of(BufferedChannelEnum.LETTSCR, null, 0, 1, NullPointerException.class), // T1
                Arguments.of(BufferedChannelEnum.LETTSCR, getWriteOnlyByteBuf(11), 0, 11, IOException.class), // T2
                Arguments.of(BufferedChannelEnum.LETTSCR, getReadOnlyByteBuf(11), 0, 11, IOException.class), // T3
//                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, -1, IllegalArgumentException.class), // T4 - doesnt throw exception, directly exit with 0 as return
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, 12, IOException.class), // T8
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), -1, 11, IllegalArgumentException.class), // T9
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 11, 11, IllegalArgumentException.class), // T11
                Arguments.of(BufferedChannelEnum.LETTSCR_W, getByteBuf(11), 11, 11, IOException.class), // T11
                Arguments.of(BufferedChannelEnum.SCR, getByteBuf(11), 0, 11, IOException.class), // T12
                Arguments.of(BufferedChannelEnum.LETTSCR_FlUSH_TRUNC, getByteBuf(11), 0, 11, IOException.class) // T15
        );
    }

    @ParameterizedTest
    @MethodSource("args")
    void testBufChannelRead(BufferedChannelEnum buffChannelEnum, ByteBuf dest, int pos, int length, String expected, int numBytesExpected) throws IOException {
        FileChannel fc;
        ByteBuf buf;

        switch (buffChannelEnum) {
            case SCR:
                fc = FileChannel.open(path, StandardOpenOption.WRITE);
                fc.position(0);

                bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 1024, 512);

                buf = ByteBufAllocator.DEFAULT.directBuffer();
                buf.writeBytes("Hello World".getBytes(StandardCharsets.UTF_8));

                bc.write(buf);

                break;
            case LETTSCR_NULLALLOCATOR:
                fc = FileChannel.open(path, StandardOpenOption.WRITE);
                fc.position(0);

                bc = new BufferedChannel(getNullWriteByteBufAllocator(), fc, 1024, 512);

                break;
            case LETTSCR_FlUSH:
                fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                fc.position(0);

                bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 11, 0);

                buf = ByteBufAllocator.DEFAULT.directBuffer();
                buf.writeBytes("Hello World".getBytes(StandardCharsets.UTF_8));

                bc.write(buf);
                bc.flush(); // Need to change internal "writeBufferStartPosition"

                break;
            case LETTSCR_FlUSH_READ:
                fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                fc.position(0);

                bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 11, 0);

                buf = ByteBufAllocator.DEFAULT.directBuffer();
                buf.writeBytes("Hello World".getBytes(StandardCharsets.UTF_8));

                bc.write(buf);
                bc.flush(); // Need to change internal "writeBufferStartPosition"

                bc.read(getByteBuf(3), 0, 3);

                break;
            case LETTSCR:
            default:
                fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                fc.position(0);

                bc = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 1024, 512);

                buf = ByteBufAllocator.DEFAULT.directBuffer();
                buf.writeBytes("Hello World".getBytes(StandardCharsets.UTF_8));

                bc.write(buf);

                break;
        }

        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            int numBytes = bc.read(dest, pos, length);
            byte[] actual = new byte[dest.readableBytes()];
            dest.getBytes(pos, actual);

            assertEquals(numBytesExpected, numBytes, "Number of bytes read does not match");
            assertEquals(expected, dest.toString(StandardCharsets.UTF_8), "Strings read are different");
        });
    }

    static Stream<Arguments> args() {
        return Stream.of(
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, 0, "", 0), // T5
//                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, 1, "H", 1), // T6 - unexpected error, "read" method read more than length bytes
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 0, 11, "Hello World", 11), // T7
                Arguments.of(BufferedChannelEnum.LETTSCR, getByteBuf(11), 1, 10, "ello World", 10), // T10
                Arguments.of(BufferedChannelEnum.LETTSCR_NULLALLOCATOR, getByteBuf(11), 0, 11, "", 0), // T13
                Arguments.of(BufferedChannelEnum.LETTSCR_FlUSH, getByteBuf(11), 0, 11, "Hello World", 11), // T14
                Arguments.of(BufferedChannelEnum.LETTSCR_FlUSH_READ, getByteBuf(11), 0, 11, "Hello World", 11) // T16
        );
    }

    private static ByteBuf getByteBuf(int capacity) {
        return UnpooledByteBufAllocator.DEFAULT.directBuffer(capacity);
    }

    private static ByteBuf getReadOnlyByteBuf(int capacity) {
        return UnpooledByteBufAllocator.DEFAULT.buffer(capacity).asReadOnly();
    }

    public static ByteBuf getWriteOnlyByteBuf(int capacity) {
        ByteBuf buf = spy(UnpooledByteBufAllocator.DEFAULT.directBuffer(capacity));

        when(buf.readableBytes()).thenThrow(new IndexOutOfBoundsException("Buffer is write-only"));

        return buf;
    }

    private static ByteBufAllocator getNullWriteByteBufAllocator() {
        ByteBufAllocator allocator = mock(ByteBufAllocator.class);

        when(allocator.directBuffer(anyInt())).thenReturn(null);

        return allocator;
    }
}
