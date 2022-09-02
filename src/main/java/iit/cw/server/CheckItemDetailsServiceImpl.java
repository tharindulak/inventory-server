package iit.cw.server;

import iit.cw.CheckQuantityResponse;
import iit.cw.CheckQuantityServiceGrpc;

public class CheckItemDetailsServiceImpl extends CheckQuantityServiceGrpc.CheckQuantityServiceImplBase {

    private InventoryServer server;

    public CheckItemDetailsServiceImpl(InventoryServer server){
        this.server = server;
    }

    @Override
    public void checkQuantity(iit.cw.CheckQuantityRequest request,
                              io.grpc.stub.StreamObserver<iit.cw.CheckQuantityResponse> responseObserver) {

        String itemId = request.getItemId();
        System.out.println("Request received..");
        double qty = getItemQuantity(itemId);
        CheckQuantityResponse response = CheckQuantityResponse
                .newBuilder()
                .setQuantity(qty)
                .build();
        System.out.println("Responding, quantity for item " + itemId + " is " + qty);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private double getItemQuantity(String accountId) {
        return server.getItemQuantity(accountId);
    }
}
