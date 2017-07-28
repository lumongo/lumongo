package org.lumongo.util.cache;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static io.grpc.Metadata.Key;

/**
 * Created by Matt Davis on 6/28/17.
 * @author mdavis
 */
public interface MetaKeys {
	Key<String> ERROR_KEY = Key.of("error", ASCII_STRING_MARSHALLER);
}
