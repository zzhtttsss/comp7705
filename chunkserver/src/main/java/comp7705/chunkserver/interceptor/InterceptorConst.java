package comp7705.chunkserver.interceptor;

import comp7705.chunkserver.common.Const;
import io.grpc.Context;
import io.grpc.Metadata;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public class InterceptorConst {

    public static final Context.Key<Metadata> METADATA = Context.key("metadata");

    public static final Metadata.Key<String> CHUNK_ID = Metadata.Key.of(Const.ChunkIdString, Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> ADDRESSES = Metadata.Key.of(Const.AddressString, Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> CHUNK_SIZE = Metadata.Key.of(Const.ChunkSizeString, Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> CHECKSUM = Metadata.Key.of(Const.CheckSumString, Metadata.ASCII_STRING_MARSHALLER);

}
