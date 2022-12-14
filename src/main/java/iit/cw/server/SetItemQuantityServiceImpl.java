package iit.cw.server;

import iit.cw.SetQuantityRequest;
import iit.cw.SetQuantityResponse;
import iit.cw.SetQuantityServiceGrpc;
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

public class SetItemQuantityServiceImpl extends SetQuantityServiceGrpc.SetQuantityServiceImplBase implements DistributedTxListener {
    private ManagedChannel channel = null;
    SetQuantityServiceGrpc.SetQuantityServiceBlockingStub clientStub = null;
    private InventoryServer server;

    private Pair<String, Double> tempDataHolder;
    private boolean transactionStatus = false;

    public SetItemQuantityServiceImpl(InventoryServer server) {
        this.server = server;
    }

    private void startDistributedTx(String accountId, double value) {
        try {
            server.getQtySetTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
            tempDataHolder = new Pair<>(accountId, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onGlobalCommit() {
        updateQuantity();
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    @Override
    public void setQuantity(iit.cw.SetQuantityRequest request,
                            io.grpc.stub.StreamObserver<iit.cw.SetQuantityResponse> responseObserver) {

        String itemId = request.getItemId();
        double quantity = request.getQuantity();
        if (server.isLeader()) {
            // Act as primary
            try {
                System.out.println("Updating item quantity: Primary");
                startDistributedTx(itemId, quantity);
                boolean isSecondaryUpdate = updateSecondaryServers(itemId, quantity);
                System.out.println("going to perform");
                boolean isTxPerformed = false;
                if (quantity > 0) {
                    isTxPerformed = ((DistributedTxCoordinator) server.getQtySetTransaction()).perform();
                } else {
                    ((DistributedTxCoordinator) server.getQtySetTransaction()).sendGlobalAbort();
                }
                if (isTxPerformed && isSecondaryUpdate) {
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
                    ((DistributedTxParticipant) server.getQtySetTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) server.getQtySetTransaction()).voteAbort();
                }
            } else {
                SetQuantityResponse response = callPrimary(itemId, quantity);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }
        SetQuantityResponse response = SetQuantityResponse.newBuilder().setStatus(transactionStatus).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    private void updateQuantity() {
        if (tempDataHolder != null) {
            String itemId = tempDataHolder.getKey();
            double quantity = tempDataHolder.getValue();
            server.setItemQuantity(itemId, quantity);
            System.out.println("Item " + itemId + " updated to quantity " + quantity + " committed");
            tempDataHolder = null;
        }
    }

    private SetQuantityResponse callServer(String itemId, double qty, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = SetQuantityServiceGrpc.newBlockingStub(channel);

        SetQuantityRequest request = SetQuantityRequest
                .newBuilder()
                .setItemId(itemId)
                .setQuantity(qty)
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        SetQuantityResponse response = clientStub.setQuantity(request);
        return response;
    }

    private SetQuantityResponse callPrimary(String itemId, double qty) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(itemId, qty, false, IPAddress, port);
    }

    private boolean updateSecondaryServers(String itemId, double qty) throws KeeperException, InterruptedException {
        System.out.println("Updating other servers");
        List<String[]> othersData = server.getOthersData();
        boolean isUpdated = false;
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            SetQuantityResponse res = callServer(itemId, qty, true, IPAddress, port);
            isUpdated = res.getStatus();
        }
        return (othersData.size() == 0 || isUpdated);
    }

}
