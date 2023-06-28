package comp7705.chunkserver.interceptor;

import io.grpc.*;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public class MetadataInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {

        Context context = Context.current().withValue(InterceptorConst.METADATA, metadata);
        return Contexts.interceptCall(context, serverCall, metadata, serverCallHandler);

    }
}
