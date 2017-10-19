package ir.ac.sbu.graph.ktruss.others;

import edu.mit.ll.graphulo.Graphulo;
import ir.ac.sbu.graph.utils.Log;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 */
public class KTrussGraphulo {
    private String instanceName;
    private String zkServers;
    private String username;
    private AuthenticationToken token;
    private Graphulo graphulo;


    public KTrussGraphulo(String instanceName, String zkServers, String username, AuthenticationToken token) {
        this.instanceName = instanceName;
        this.zkServers = zkServers;
        this.username = username;
        this.token = token;
    }

    public void setup() throws AccumuloSecurityException, AccumuloException {
        ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zkServers);
        Instance instance = new ZooKeeperInstance(cc);
        Connector connector = instance.getConnector(username, token);
        graphulo = new Graphulo(connector, token);
    }

    public void run(String tableName, int k) {
        String newTable = tableName + "" + System.currentTimeMillis();
        long ts = System.currentTimeMillis();
        long nnz = graphulo.kTrussAdj_Smart(tableName, newTable, k, null,
                true, Authorizations.EMPTY, "", Integer.MAX_VALUE,null);
        Log.log("nnz: " + nnz, ts, System.currentTimeMillis());
    }

    public void fillTable(String tableName, String fileName, SortedSet<Text> splits) throws Exception {
        Connector conn = graphulo.getConnector();
        if (conn.tableOperations().exists(tableName)) {
            conn.tableOperations().delete(tableName);
        }
        conn.tableOperations().create(tableName);
        if (splits != null)
            conn.tableOperations().addSplits(tableName, splits);

        BufferedReader br = new BufferedReader(new FileReader(new File(fileName)));
        BatchWriterConfig config = new BatchWriterConfig();
        BatchWriter writer = conn.createBatchWriter(tableName, config);
        int batchSize = 1000;
        int i = 0;
        Mutation m;
        Value value = new Value("1".getBytes(StandardCharsets.UTF_8));
        Key key;

        while (true) {
            String line = br.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("#")) {
                continue;
            }
            String[] edge = line.split("\\s+");
            try {
                Long.parseLong(edge[0]);
            } catch (Exception e) {
                edge = line.split(",");
                try {
                    Long.parseLong(edge[0]);
                } catch (Exception e1) {
                    Log.log("invalid input: " + line);
                }
            }

            key = new Key(edge[0], "", edge[1]);
            m = new Mutation(key.getRowData().toArray());
            m.put(key.getColumnFamilyData().toArray(), key.getColumnQualifierData().toArray(),
                    key.getColumnVisibilityParsed(), value.get());
            writer.addMutation(m);
            if ( ++ i % batchSize == 0) {
                writer.flush();
            }
        }
        writer.flush();
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1)
            Log.log("Enter method: FILL, RUN");

        if (args.length < 2)
            Log.log("Enter zk address in the second argument");
        String zk = args[1];

        if (args.length < 3)
            Log.log("Enter instance name in the third argument");
        String instanceName = args[2];

        if (args.length < 4)
            Log.log("Enter username in the fourth argument");
        String username = args[3];

        if (args.length < 5)
            Log.log("Enter password");
        String password = args[4];

        if (args.length < 6)
            Log.log("Enter table name to create");
        String tableName = args[5];


        KTrussGraphulo kTrussGraphulo = new KTrussGraphulo(instanceName, zk, username, new PasswordToken(password));
        kTrussGraphulo.setup();

        switch (args[0].toUpperCase()) {
            case "FILL":
                if (args.length < 7)
                    Log.log("Enter fileName to fill in tables");
                String fileName = args[6];

                kTrussGraphulo.fillTable(tableName, fileName, createSplits(fileName));

                break;
            case "RUN":
                int k = 4;
                if (args.length < 7)
                    Log.log("Enter fileName to fill in tables");
                else
                    k = Integer.parseInt(args[6]);
                kTrussGraphulo.run(tableName, k);
        }
    }

    private static SortedSet<Text> createSplits(String fileName) throws IOException {

        File file = new File(fileName);
        long size = file.length();
        long batchSizeBytes = size / 1000;

        BufferedReader br = new BufferedReader(new FileReader(file));

        SortedSet<Text> set = new TreeSet<>();
        long countBytes = 0;
        while (true) {
            String line = br.readLine();

            if (line == null) break;

            if (line.startsWith("#")) continue;

            String[] edge = line.split("\\s+");
            long e1 = Long.parseLong(edge[0]);
            countBytes += line.getBytes().length;

            if (countBytes > batchSizeBytes) {
                countBytes = 0;
                set.add(new Text(e1 + ""));
                System.out.println("split:" + e1);
            }
        }

        return set;
    }
}
