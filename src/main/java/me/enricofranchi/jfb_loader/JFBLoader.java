/* Copyright */
package me.enricofranchi.jfb_loader;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * User: enrico
 * Package: PACKAGE_NAME
 * Date: 12/15/11
 * Time: 4:16 PM
 */
public class JFBLoader {

    private static final String DB_PATH = "data/graph.db";
    private static final String SOCIAL_GRAPH_FILE = "../minas_datasets/uni-socialgraph-anonymized";
    private GraphDatabaseService graphDb;
    private Index<Node> nodeIndex;

    static private final String NODES_INDEX_KEY = "nodes";
    static private final String NODE_ID = "node_id";

    public JFBLoader(final GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        IndexManager indexManager = graphDb.index();
        nodeIndex = indexManager.forNodes(NODES_INDEX_KEY);

        registerShutdownHook(graphDb);
    }

    private NodeWithFriends parseLine(String line) {
        final List<String> friendsAsString = new LinkedList<String>();
        final StringTokenizer tokenizer = new StringTokenizer(line);
        final String nodeIdAsString = tokenizer.nextToken();
        final String _hitsNumber = tokenizer.nextToken();

        while (tokenizer.hasMoreTokens()) {
            final String nextToken = tokenizer.nextToken();
            friendsAsString.add(nextToken);
        }
        return NodeWithFriends.create(nodeIdAsString, friendsAsString);
    }

    private void load(Reader file) throws IOException {
        final BufferedReader bufferedReader = new BufferedReader(file);
        String currentLine;
        while ((currentLine = bufferedReader.readLine()) != null) {
            NodeWithFriends currentNode = parseLine(currentLine);
            loadNode(currentNode);
        }

    }

    private void loadNode(final NodeWithFriends currentNode) {
        Transaction tx = graphDb.beginTx();
        try {

            Node node = locateNode(currentNode.nodeIdentifier);
            for (int friendIdentifier : currentNode.friendIdentifiers) {
                Node other = locateNode(friendIdentifier);
                node.createRelationshipTo(other, Friend.FRIEND);
            }
        } finally {
            tx.finish();
        }
    }

    /**
     * Returns the node with the given name.
     * <p/>
     * Notice that the search is performed
     *
     * @param nodeIdentifier
     * @return
     */
    private Node locateNode(final int nodeIdentifier) {
        Node node = nodeIndex.get(NODE_ID, nodeIdentifier).getSingle();
        if (node == null) {
            node = graphDb.createNode();
            node.setProperty(NODE_ID, nodeIdentifier);
            nodeIndex.add(node, NODE_ID, nodeIdentifier);
        }
        return node;
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    private static class NodeWithFriends {
        int nodeIdentifier;
        int friendIdentifiers[];

        private NodeWithFriends(final int nodeIdentifier, final int[] friendIdentifiers) {
            this.nodeIdentifier = nodeIdentifier;
            this.friendIdentifiers = friendIdentifiers;
        }

        public static NodeWithFriends create(final String nodeIdAsString, final List<String> friendsAsString) {
            int nodeIdentifier = Integer.parseInt(nodeIdAsString);
            int friendIdentifiers[] = new int[friendsAsString.size()];
            int counter = 0;

            for (String friendIdAsString : friendsAsString) {
                friendIdentifiers[counter++] = Integer.parseInt(friendIdAsString);
            }

            return new NodeWithFriends(nodeIdentifier, friendIdentifiers);
        }


    }

    public static void main(String[] argv) throws IOException {
        String dbPath = DB_PATH;
        String source = SOCIAL_GRAPH_FILE;

        switch (argv.length) {
            case 2:
                dbPath = argv[1];
            case 1:
                source = argv[0];
        }

        EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase(dbPath);
        JFBLoader loader = new JFBLoader(graphDb);
        loader.load(new FileReader(source));
    }


    private static enum Friend implements RelationshipType {
        FRIEND
    }

}
