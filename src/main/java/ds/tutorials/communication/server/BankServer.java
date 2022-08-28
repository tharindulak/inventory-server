package ds.tutorials.communication.server;

import ds.tutorials.sycnhronization.DistributedLock;
import ds.tutorials.sycnhronization.DistributedTx;
import ds.tutorials.sycnhronization.DistributedTxCoordinator;
import ds.tutorials.sycnhronization.DistributedTxParticipant;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class BankServer {
    private DistributedLock leaderLock;
    private AtomicBoolean isLeader = new AtomicBoolean(false);
    private byte[] leaderData;
    private int serverPort;
    private Map<String, Double> accounts = new HashMap();
    private DistributedTx transaction;
    private SetBalanceServiceImpl setBalanceService;
    private CheckBalanceServiceImpl checkBalanceService;

    public static String buildServerData(String IP, int port) {
        StringBuilder builder = new StringBuilder();
        builder.append(IP).append(":").append(port);
        return builder.toString();
    }

    public BankServer(String host, int port) throws InterruptedException, IOException, KeeperException {
        this.serverPort = port;
        leaderLock = new DistributedLock("BankServerDisTsx", buildServerData(host, port));
        setBalanceService = new SetBalanceServiceImpl(this);
        checkBalanceService = new CheckBalanceServiceImpl(this);
        transaction = new DistributedTxParticipant(setBalanceService);
    }

    public DistributedTx getTransaction() {
        return transaction;
    }

    private void tryToBeLeader() throws KeeperException, InterruptedException {
        Thread leaderCampaignThread = new Thread(new LeaderCampaignThread());
        leaderCampaignThread.start();
    }

    public void startServer() throws IOException, InterruptedException, KeeperException {
        Server server = ServerBuilder
                .forPort(serverPort)
                .addService(checkBalanceService)
                .addService(setBalanceService)
                .build();
        server.start();
        System.out.println("BankServer Started and ready to accept requests on port " + serverPort);

        tryToBeLeader();
        server.awaitTermination();
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    private synchronized void setCurrentLeaderData(byte[] leaderData) {
        this.leaderData = leaderData;
    }

    public synchronized String[] getCurrentLeaderData() {
        return new String(leaderData).split(":");
    }

    public void setAccountBalance(String accountId, double value) {
        accounts.put(accountId, value);
    }

    public double getAccountBalance(String accountId) {
        Double value = accounts.get(accountId);
        return (value != null) ? value : 0.0;
    }

    public List<String[]> getOthersData() throws KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();

        for (byte[] data : othersData) {
            String[] dataStrings = new String(data).split(":");
            result.add(dataStrings);
        }
        return result;
    }

    class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;

        @Override
        public void run() {
            System.out.println("Starting the leader Campaign");

            try {
                boolean leader = leaderLock.tryAcquireLock();

                while (!leader) {
                    byte[] leaderData = leaderLock.getLockHolderData();
                    if (currentLeaderData != leaderData) {
                        currentLeaderData = leaderData;
                        setCurrentLeaderData(currentLeaderData);
                    }
                    Thread.sleep(10000);
                    leader = leaderLock.tryAcquireLock();
                }
                System.out.println("I got the leader lock. Now acting as primary");
                currentLeaderData = null;
                beTheLeader();
            } catch (Exception e) {
            }
        }
    }

    private void beTheLeader() {
        System.out.println("I got the leader lock. Now actingas primary");
        isLeader.set(true);
        transaction = new DistributedTxCoordinator(setBalanceService);
    }

    public static void main(String[] args) throws Exception {
        DistributedTx.setZooKeeperURL("localhost:2181");
        if (args.length != 1) {
            System.out.println("Usage executable-name <port>");
        }

        int serverPort = Integer.parseInt(args[0]);
        DistributedLock.setZooKeeperURL("localhost:2181");

        BankServer server = new BankServer("localhost", serverPort);
        server.startServer();
    }
}
