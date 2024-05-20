import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class WriteCacheTest {
    private WriteCache writeCache;
    private ByteBufAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = mock(ByteBufAllocator.class);
        when(allocator.buffer(anyInt(), anyInt())).thenAnswer(invocation -> Unpooled.buffer(invocation.getArgument(0), invocation.getArgument(1)));
        writeCache = new WriteCache(allocator, 1024 * 1024); // 1MB cache size
    }

    @AfterEach
    void tearDown() {
        writeCache.close();
    }

    @Test
    void testPutAndGet() {
        long ledgerId = 1L;
        long entryId = 1L;
        ByteBuf entry = Unpooled.buffer();
        entry.writeBytes(new byte[]{1, 2, 3, 4});

        assertTrue(writeCache.put(ledgerId, entryId, entry));

        ByteBuf cachedEntry = writeCache.get(ledgerId, entryId);
        assertNotNull(cachedEntry);
        assertEquals(entry, cachedEntry);
    }

    @Test
    void testCacheSizeAfterPut() {
        long ledgerId = 1L;
        long entryId = 1L;
        ByteBuf entry = Unpooled.buffer();
        entry.writeBytes(new byte[]{1, 2, 3, 4});

        writeCache.put(ledgerId, entryId, entry);

        assertEquals(entry.readableBytes(), writeCache.size());
    }

    @Test
    void testClearCache() {
        long ledgerId = 1L;
        long entryId = 1L;
        ByteBuf entry = Unpooled.buffer();
        entry.writeBytes(new byte[]{1, 2, 3, 4});

        writeCache.put(ledgerId, entryId, entry);
        writeCache.clear();

        assertNull(writeCache.get(ledgerId, entryId));
        assertEquals(0, writeCache.size());
    }

    @Test
    void testHasEntry() {
        long ledgerId = 1L;
        long entryId = 1L;
        ByteBuf entry = Unpooled.buffer();
        entry.writeBytes(new byte[]{1, 2, 3, 4});

        writeCache.put(ledgerId, entryId, entry);

        assertTrue(writeCache.hasEntry(ledgerId, entryId));
        assertFalse(writeCache.hasEntry(ledgerId, entryId + 1));
    }

    @Test
    void testDeleteLedger() {
        long ledgerId = 1L;
        long entryId = 1L;
        ByteBuf entry = Unpooled.buffer();
        entry.writeBytes(new byte[]{1, 2, 3, 4});

        writeCache.put(ledgerId, entryId, entry);
        writeCache.deleteLedger(ledgerId);

        assertNotNull(writeCache.get(ledgerId, entryId));
    }

    @Test
    void testGetLastEntry() {
        long ledgerId = 1L;
        ByteBuf entry1 = Unpooled.buffer();
        entry1.writeBytes(new byte[]{1, 2, 3, 4});
        ByteBuf entry2 = Unpooled.buffer();
        entry2.writeBytes(new byte[]{5, 6, 7, 8});

        writeCache.put(ledgerId, 1L, entry1);
        writeCache.put(ledgerId, 2L, entry2);

        ByteBuf lastEntry = writeCache.getLastEntry(ledgerId);
        assertNotNull(lastEntry);
        assertEquals(entry2, lastEntry);
    }

    @Test
    void testForEach() throws IOException {
        long ledgerId1 = 1L;
        long entryId1 = 1L;
        long ledgerId2 = 2L;
        long entryId2 = 1L;
        ByteBuf entry1 = Unpooled.buffer();
        entry1.writeBytes(new byte[]{1, 2, 3, 4});
        ByteBuf entry2 = Unpooled.buffer();
        entry2.writeBytes(new byte[]{5, 6, 7, 8});

        writeCache.put(ledgerId1, entryId1, entry1);
        writeCache.put(ledgerId2, entryId2, entry2);

        WriteCache.EntryConsumer consumer = mock(WriteCache.EntryConsumer.class);
        writeCache.forEach(consumer);

        verify(consumer).accept(eq(ledgerId1), eq(entryId1), any(ByteBuf.class));
        verify(consumer).accept(eq(ledgerId2), eq(entryId2), any(ByteBuf.class));
    }
}
