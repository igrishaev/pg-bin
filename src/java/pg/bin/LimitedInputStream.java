package pg.bin;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/*
    Stolen from Guava with small changes.
 */
public class LimitedInputStream extends FilterInputStream {

    private long left;
    private long mark = -1;

    public LimitedInputStream(final InputStream in, final long limit) {
        super(in);
        left = limit;
    }

    @Override
    public int available() throws IOException {
        return (int) Math.min(in.available(), left);
    }

    // it's okay to mark even if mark isn't supported, as reset won't work
    @Override
    public synchronized void mark(final int readLimit) {
        in.mark(readLimit);
        mark = left;
    }

    @Override
    public int read() throws IOException {
        if (left == 0) {
            return -1;
        }

        int result = in.read();
        if (result != -1) {
            --left;
        }
        return result;
    }

    @Override
    public int read(final byte[] b, final int off, int len) throws IOException {
        if (left == 0) {
            return -1;
        }

        len = (int) Math.min(len, left);
        int result = in.read(b, off, len);
        if (result != -1) {
            left -= result;
        }
        return result;
    }

    @Override
    public synchronized void reset() throws IOException {
        if (!in.markSupported()) {
            throw new IOException("Mark not supported");
        }
        if (mark == -1) {
            throw new IOException("Mark not set");
        }

        in.reset();
        left = mark;
    }

    @Override
    public long skip(long n) throws IOException {
        n = Math.min(n, left);
        long skipped = in.skip(n);
        left -= skipped;
        return skipped;
    }

    @Override
    public void close() {
        // keep the parent stream open
    }
}
