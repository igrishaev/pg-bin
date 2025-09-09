package org.pg.copy;

import java.io.IOException;
import java.io.InputStream;

public class OpenInputStream extends InputStream {

    private final InputStream in;

    public OpenInputStream(final InputStream in) {
        this.in = in;
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    @Override
    public void close() {}
}
