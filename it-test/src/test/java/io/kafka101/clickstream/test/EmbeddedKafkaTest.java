package io.kafka101.clickstream.test;

import io.confluent.kafka.schemaregistry.RestApp;
import kafka.admin.AdminUtils;
import kafka.api.FixedPortTestUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class EmbeddedKafkaTest {

    protected static String zkConnect;
    protected static String kafkaConnect;
    protected static String restConnect;

    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static KafkaServer kafkaServer;
    private static ZkUtils zkUtils;
    private static ZkConnection zkConnection;
    private static RestApp schemaRegistry;

    private static List<KafkaServer> servers = new ArrayList();

    @BeforeClass
    public static void setUp() throws Exception {
        zkServer = new EmbeddedZookeeper();
        zkConnect = "127.0.0.1:" + zkServer.port();
        zkConnection = new ZkConnection(zkConnect);
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = new ZkUtils(zkClient, zkConnection, false);

        // setup Broker
        Properties props = FixedPortTestUtils.createBrokerConfigs(1, zkConnect, true, false).apply(0);
        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        servers.add(kafkaServer);
        kafkaConnect = kafkaServer.config().hostName() + ":" + kafkaServer.config().port();

        // setup Schema-Registry
        schemaRegistry = new RestApp(8081, zkConnect, "_schemas");
        schemaRegistry.start();
        restConnect = schemaRegistry.restConnect;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        schemaRegistry.stop();
        zkClient.close();
        kafkaServer.shutdown();
        zkServer.shutdown();
        servers.clear();
    }

    protected void createTopic(String topic) {
        Properties properties = new Properties();
        properties.put("cleanup.policy", "compact");
        AdminUtils.createTopic(zkUtils, topic, 1, 1, properties);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0,
                10000);
    }
}