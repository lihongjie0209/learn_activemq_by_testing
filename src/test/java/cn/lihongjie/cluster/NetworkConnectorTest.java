package cn.lihongjie.cluster;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.ConditionalNetworkBridgeFilterFactory;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;
import org.nutz.log.Log;

import javax.jms.*;
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.nutz.log.Logs.get;

/**
 * @author 982264618@qq.com
 */
public class NetworkConnectorTest {

	private static Log logger = get();
	private static String b1URL;
	private static String b2URL;
	private static ActiveMQConnectionFactory b1Facotry;
	private static ActiveMQConnectionFactory b2Facotry;
	private static BrokerService b1;
	private static BrokerService b2;

	private static void logInfo(Description description, String status, long nanos) {
		String testName = description.getMethodName();
		logger.info(format("Test %s %s, spent %d microseconds",
				testName, status, NANOSECONDS.toMicros(nanos)));
	}

	@Rule
	public Stopwatch stopwatch = new Stopwatch() {
		@Override
		protected void succeeded(long nanos, Description description) {
			logInfo(description, "succeeded", nanos);
		}

		@Override
		protected void failed(long nanos, Throwable e, Description description) {
			logInfo(description, "failed", nanos);
		}


		@Override
		protected void finished(long nanos, Description description) {
			//			logInfo(description, "finished", nanos);
		}
	};

	@BeforeClass
	public static void setUp() throws Exception {


		b1URL = "tcp://localhost:11111";
		b1 = getBrokerService("b1", b1URL);
		b1.setNetworkConnectorURIs(new String[]{"static:(tcp://localhost:22222)"});
		b1.start();

		b2URL = "tcp://localhost:22222";
		b2 = getBrokerService("b2", b2URL);
		b2.setNetworkConnectorURIs(new String[]{"static:(tcp://localhost:11111)"});
		b2.start();


		b1Facotry = new ActiveMQConnectionFactory(b1URL);
		b2Facotry = new ActiveMQConnectionFactory(b2URL);


	}

	private static BrokerService getBrokerService(String brokerName, String url) throws Exception {
		BrokerService b1 = new BrokerService();

		b1.setBrokerName(brokerName);
		b1.setPersistent(false);

		PolicyMap policyMap = new PolicyMap();


		PolicyEntry policyEntry = new PolicyEntry();
		policyEntry.setQueue(">");
		policyEntry.setEnableAudit(false);
		ConditionalNetworkBridgeFilterFactory conditionalNetworkBridgeFilterFactory = new ConditionalNetworkBridgeFilterFactory();
		conditionalNetworkBridgeFilterFactory.setReplayWhenNoConsumers(true);
		policyEntry.setNetworkBridgeFilterFactory(conditionalNetworkBridgeFilterFactory);
		policyMap.setPolicyEntries(Arrays.asList(policyEntry));
		b1.setDestinationPolicy(policyMap);
		TransportConnector connector = new TransportConnector();

		connector.setUri(new URI(url));
		b1.setTransportConnectors(Arrays.asList(connector));
		return b1;
	}

	@Test
	public void testSendOnB1ReceiveOnB2() throws Exception {


		ActiveMQQueue queue = new ActiveMQQueue(UUID.randomUUID().toString());
		Connection b1Connection = b1Facotry.createConnection();
		b1Connection.start();
		Session b1ConnectionSession = b1Connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		MessageProducer producer = b1ConnectionSession.createProducer(queue);

		producer.send(b1ConnectionSession.createTextMessage("aaa"));

		b1ConnectionSession.close();
		b1Connection.close();


		Connection b2Connection = b2Facotry.createConnection();
		b2Connection.start();
		Session b2ConnectionSession = b2Connection.createSession(false, Session.AUTO_ACKNOWLEDGE);


		MessageConsumer consumer = b2ConnectionSession.createConsumer(queue);

		TextMessage message = (TextMessage) consumer.receive();

		logger.info(message);
		b2ConnectionSession.close();
		b2Connection.close();


		Assert.assertThat(message.getText(), Is.is("aaa"));


	}


	@Test
	public void testMessageReplay() throws Exception {


		// b1发送10个消息
		ActiveMQQueue queue = new ActiveMQQueue(UUID.randomUUID().toString());
		Connection b1Connection = b1Facotry.createConnection();
		b1Connection.start();
		Session b1ConnectionSession = b1Connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		MessageProducer producer = b1ConnectionSession.createProducer(queue);

		for (int i = 0; i < 10; i++) {

			producer.send(b1ConnectionSession.createTextMessage("aaa"));
		}

		b1ConnectionSession.close();
		b1Connection.close();


		// b2会把这个10个消息全部拿走, pre-fetch
		Connection b2Connection = b2Facotry.createConnection();
		b2Connection.start();
		Session b2ConnectionSession = b2Connection.createSession(false, Session.AUTO_ACKNOWLEDGE);


		MessageConsumer consumer = b2ConnectionSession.createConsumer(queue);
		for (int i = 0; i < 5; i++) {

			TextMessage message = (TextMessage) consumer.receive();
			logger.info("b2收到消息");
			logger.info(message);
		}

		// b2下线, 那么b2 prefetch 的消息会保留在b2中, 而消费者会failover到b1, 那么b2重启之后就没有消费者了
		b2ConnectionSession.close();
		b2Connection.close();

		// 此时消息应该回流到了b1


		Connection b1Connection2 = b1Facotry.createConnection();
		b1Connection2.start();
		Session b1ConnectionSession2 = b1Connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

		MessageConsumer consumer2 = b1ConnectionSession2.createConsumer(queue);

		for (int i = 0; i < 5; i++) {
			logger.info("b1收到消息");
			logger.info(consumer2.receive());
		}

		b1ConnectionSession2.close();
		b1Connection2.close();






	}


}
