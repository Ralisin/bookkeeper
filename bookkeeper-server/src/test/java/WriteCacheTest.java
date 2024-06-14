import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class WriteCacheTest {
    enum WriteCacheEnum {
        EMPTY_WRITECACHE,
        SMALL_WRITECACHE,
        BIG_WRITECACHE,
        DOUBLE_WRITECACHE
    }

    @ParameterizedTest
    @MethodSource("argsError")
    public void testWriteCacheError(ByteBufAllocator allocator, long maxCacheSize, int maxSegmentSize, long ledgerId, long entryId, ByteBuf entry, Class<? extends Throwable> expectedException) {


        try (WriteCache writeCache = new WriteCache(allocator, maxCacheSize, maxSegmentSize)) {
            boolean result = writeCache.put(ledgerId, entryId, entry);

            assertFalse(result);
        } catch (Exception e) {
            assertEquals(expectedException, e.getClass());

            return;
        }

        assertNull(expectedException);
    }

    static Stream<Arguments> argsError() {
        return Stream.of(
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024, -1, 0, getEmptyByteBuf(11), IllegalArgumentException.class), // T1
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024, 0, -1, getEmptyByteBuf(11), IllegalArgumentException.class), // T2
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024, 0, 0, null, NullPointerException.class), // T3
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024, 0, 0, getWriteOnlyByteBuf(11, ""), IndexOutOfBoundsException.class) // T9
//                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024, 0, 0, getDeallocatedByteBuf(11), IllegalArgumentException.class) // T10
        );
    }

    @ParameterizedTest
    @MethodSource("args")
    public void testWriteCache(ByteBufAllocator allocator, long maxCacheSize, int maxSegmentSize, WriteCacheEnum writeCacheEnum, long ledgerId, long entryId, ByteBuf entry, boolean bool, int size, int count) {
        try (WriteCache writeCache = new WriteCache(allocator, maxCacheSize, maxSegmentSize)) {
            switch (writeCacheEnum) {
                case EMPTY_WRITECACHE:
                    break;
                case SMALL_WRITECACHE:
                    writeCache.put(1, 1, getByteBuf(5, "Hello"));
                    break;
                case BIG_WRITECACHE:
                    writeCache.put(1, 1, getByteBuf(64));
                    break;
                case DOUBLE_WRITECACHE:
                    writeCache.put(0, 0, getByteBuf(64));
                    writeCache.put(0, 1, getByteBuf(64));
                    break;
            }

            boolean result = writeCache.put(ledgerId, entryId, entry);

            assertEquals(bool, result);
            assertEquals(size, writeCache.size());
            assertEquals(count, writeCache.count());
        } catch (Exception e) {
            assertNull(e);
        }
    }

    @Test
    public void test() {
        try (WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024)) {
            writeCache.put(1, 1, getByteBuf(5, "Hello"));

            boolean result = writeCache.put(1, 0, getByteBuf(11, "Hello World"));

            assertTrue(result);
            assertEquals(16, writeCache.size());
            assertEquals(2, writeCache.count());
        }
    }

    static Stream<Arguments> args() {
        return Stream.of(
//                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024, WriteCacheEnum.EMPTY_WRITECACHE, 0, 0, getEmptyByteBuf(11), true, 0, 1), // T4
//                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024, WriteCacheEnum.EMPTY_WRITECACHE, 0, 0, getByteBuf(11, "Hello World"), true, 11, 1), // T5
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024, WriteCacheEnum.SMALL_WRITECACHE, 1, 0, getByteBuf(11, "Hello World"), true, 16, 2) // T6
//                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024, WriteCacheEnum.SMALL_WRITECACHE, 1, 1, getByteBuf(11, "Hello World"), true, 16, 2), // T7
//                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 1024, WriteCacheEnum.EMPTY_WRITECACHE, 1, 1, getReadOnlyByteBuf(11, "Hello World"), true, 11, 1), // T8
//                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 10, 1024, WriteCacheEnum.EMPTY_WRITECACHE, 0, 0, getByteBuf(11, "Hello World"), false, 0, 0), // T11

//                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 5L * 1024, 128, WriteCacheEnum.BIG_WRITECACHE, 0, 0, getByteBuf(65), false, 64, 1), // T12
//                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 128, 64, WriteCacheEnum.DOUBLE_WRITECACHE, 0, 2, getByteBuf(64), false, 128, 2) // T13
        );
    }

    private static ByteBuf getEmptyByteBuf(int capacity) {
        return UnpooledByteBufAllocator.DEFAULT.directBuffer(capacity);
    }

    private static ByteBuf getByteBuf(int capacity, String str) {
        ByteBuf buf = Unpooled.buffer(capacity);
        buf.writeBytes(str.getBytes());

        return buf;
    }

    private static ByteBuf getByteBuf(int capacity) {
        ByteBuf buf = Unpooled.buffer(capacity);
        buf.writeBytes(new byte[capacity]);

        return buf;
    }

    private static ByteBuf getReadOnlyByteBuf(int capacity, String str) {
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.buffer(capacity);
        buf.writeBytes(str.getBytes());

        buf.asReadOnly();

        return buf;
    }

    public static ByteBuf getWriteOnlyByteBuf(int capacity, String str) {
        ByteBuf buf = spy(UnpooledByteBufAllocator.DEFAULT.directBuffer(capacity));
        buf.writeBytes(str.getBytes());

        when(buf.readableBytes()).thenThrow(new IndexOutOfBoundsException("Buffer is write-only"));

        return buf;
    }

    private static ByteBuf getDeallocatedByteBuf(int capacity) {
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.directBuffer(capacity);

        buf.release();

        return buf;
    }
}
