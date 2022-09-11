package iit.cw.server;

import iit.cw.*;
import iit.cw.synchronization.DistributedTxCoordinator;
import iit.cw.synchronization.DistributedTxListener;
import iit.cw.synchronization.DistributedTxParticipant;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class OrderItemServiceImpl extends OrderItemServiceGrpc.OrderItemServiceImplBase implements DistributedTxListener {
    private ManagedChannel channel = null;
    OrderItemServiceGrpc.OrderItemServiceBlockingStub clientStub = null;
    private InventoryServer server;

    private Pair<String, Double> tempDataHolder;
    private boolean transactionStatus = false;

    public OrderItemServiceImpl(InventoryServer server) {
        this.server = server;
    }

    private void startDistributedTx(String accountId, double value) {
        try {
            server.getOrderItemTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
            tempDataHolder = new Pair<>(accountId, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onGlobalCommit() {
        placeItemOrder();
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    @Override
    public void orderItem(OrderItemRequest request, io.grpc.stub.StreamObserver<OrderItemResponse> responseObserver) {

        String itemId = request.getItemId();
        double quantity = request.getQuantity();
        if (server.isLeader()) {
            // Act as primary
            try {
                System.out.println("Ordering the item: Primary");
                startDistributedTx(itemId, quantity);
                updateSecondaryServers(itemId, quantity);
                System.out.println("going to perform");
                boolean isTxPerformed = false;
                if (quantity > 0) {
                    isTxPerformed = ((DistributedTxCoordinator) server.getOrderItemTransaction()).perform();
                } else {
                    ((DistributedTxCoordinator) server.getOrderItemTransaction()).sendGlobalAbort();
                }
                if (isTxPerformed) {
                    transactionStatus = true;
                }
            } catch (Exception e) {
                System.out.println("Error while updating the item quantity " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Updating item quantity on secondary, on Primary's command");
                startDistributedTx(itemId, quantity);
                if (quantity != 0.0d) {
                    ((DistributedTxParticipant) server.getOrderItemTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) server.getOrderItemTransaction()).voteAbort();
                }
            } else {
                OrderItemResponse response = callPrimary(itemId, quantity);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }
        OrderItemResponse response = OrderItemResponse.newBuilder().setStatus(transactionStatus).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    private void placeItemOrder() {
        if (tempDataHolder != null) {
            String itemId = tempDataHolder.getKey();
            double quantity = tempDataHolder.getValue();
            double remainingQty = server.getItemQuantity(itemId) - quantity;
            server.setItemQuantity(itemId, remainingQty);
            System.out.println("Order placed for " + itemId + " with a quantity of " + quantity + ".");
            tempDataHolder = null;
        }
    }

    private OrderItemResponse callServer(String itemId, double qty, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = OrderItemServiceGrpc.newBlockingStub(channel);

        OrderItemRequest request = OrderItemRequest
                .newBuilder()
                .setItemId(itemId)
                .setQuantity(qty)
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        OrderItemResponse response = clientStub.orderItem(request);
        return response;
    }

    private OrderItemResponse callPrimary(String itemId, double qty) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(itemId, qty, false, IPAddress, port);
    }

    private void updateSecondaryServers(String itemId, double qty) throws KeeperException, InterruptedException {
        System.out.println("Updating other servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            OrderItemResponse res = callServer(itemId, qty, true, IPAddress, port);
        }
    }

}
