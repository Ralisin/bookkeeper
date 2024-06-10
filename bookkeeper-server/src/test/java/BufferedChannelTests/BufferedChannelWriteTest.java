package BufferedChannelTests;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class BufferedChannelWriteTest {
    private final String path = "src/test/resources/test.txt";
    private BufferedChannel bc;

    @Test
    public void testWrite() throws IOException {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

        Path path = Paths.get(this.path);
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        fc.position(0);

        bc = new BufferedChannel(allocator, fc, 1024);

        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.directBuffer(64);
        buf.writeBytes("src/test/resources/test.txt".getBytes());
        bc.write(buf);
        bc.flush();

        buf.clear();
        bc.read(buf, -1, buf.readableBytes());
    }
}
