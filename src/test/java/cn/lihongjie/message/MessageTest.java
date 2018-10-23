package cn.lihongjie.message;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.hamcrest.core.Is;
import org.junit.*;
import org.nutz.log.Log;

import javax.jms.*;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.nutz.log.Logs.get;

/**
 * @author 982264618@qq.com
 */
public class MessageTest {


	private static Log logger = get();

	private static String URL = "vm://embedded-broker";

	@Rule
	public static EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

	private static ConnectionFactory connectionFactory;
	private static Connection connection;

	private ActiveMQQueue testQueue;
	private static ExecutorService threadPool;
	private ActiveMQQueue exclusiveQueue;


	@BeforeClass
	public static void init() throws JMSException {
		broker.start();
		connectionFactory = new ActiveMQConnectionFactory(broker.getVmURL(false));
		connection = connectionFactory.createConnection();
		connection.start();

		threadPool = Executors.newCachedThreadPool();


	}


	@AfterClass
	public static void destroy() throws JMSException {


	}


	@Before
	public void setUp() throws Exception {

		testQueue = new ActiveMQQueue(UUID.randomUUID().toString());
		exclusiveQueue = new ActiveMQQueue("TEST.QUEUE1111?consumer.exclusive=true");


	}


	@After
	public void tearDown() throws Exception {


		threadPool.shutdownNow();

	}


	@Test
	public void testMessageProp() throws Exception {


		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		MessageProducer producer = session.createProducer(testQueue);

		TextMessage textMessage = session.createTextMessage("test");
		String pName = "p1";
		String pValue = "aaaa";
		textMessage.setStringProperty(pName, pValue);
		producer.send(textMessage);


		MessageConsumer consumer = session.createConsumer(testQueue);


		Message message = consumer.receive();


		String property = message.getStringProperty(pName);

		Assert.assertThat(property, Is.is(pValue));


	}
}
