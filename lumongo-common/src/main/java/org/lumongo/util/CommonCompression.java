package org.lumongo.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

//copied from lumongo-storage
//need to find better place to put this to share
public class CommonCompression {
    private CommonCompression() {

    }

    public enum CompressionLevel {
        BEST(Deflater.BEST_COMPRESSION),
        NORMAL(Deflater.DEFAULT_COMPRESSION),
        FASTEST(Deflater.BEST_SPEED);

        private int level;

        CompressionLevel(int level) {
            this.level = level;
        }

        public int getLevel() {
            return level;
        }
    }

    public static byte[] compressZlib(byte[] bytes, CompressionLevel compressionLevel) {
        Deflater compressor = new Deflater();
        compressor.setLevel(compressionLevel.getLevel());
        compressor.setInput(bytes);
        compressor.finish();

        int bufferLength = Math.max(bytes.length / 10, 16);
        byte[] buf = new byte[bufferLength];
        ByteArrayOutputStream bos = new ByteArrayOutputStream(bufferLength);
        while (!compressor.finished()) {
            int count = compressor.deflate(buf);
            bos.write(buf, 0, count);
        }
        try {
            bos.close();
        }
        catch (Exception e) {

        }
        compressor.end();
        return bos.toByteArray();
    }

    public static byte[] uncompressZlib(byte[] bytes) throws IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(bytes);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(bytes.length);
        byte[] buf = new byte[1024];
        while (!inflater.finished()) {
            try {
                int count = inflater.inflate(buf);
                bos.write(buf, 0, count);
            }
            catch (DataFormatException e) {
            }
        }
        bos.close();
        inflater.end();
        return bos.toByteArray();
    }

}
