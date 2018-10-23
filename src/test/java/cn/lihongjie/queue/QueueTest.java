package cn.lihongjie.queue;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.hamcrest.core.Is;
import org.junit.*;
import org.nutz.log.Log;

import javax.jms.*;
import java.lang.IllegalStateException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.nutz.log.Logs.get;

/**
 * @author 982264618@qq.com
 */

public class QueueTest {


	private static Log logger = get();

	private static String URL = "vm://embedded-broker";

	@Rule
	public static EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

	private static ConnectionFactory connectionFactory;
	private static Connection connection;
	private Session tranSession;
	private Session session;
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

	/**
	 * 测试基本的消息队列
	 *
	 * @throws Exception
	 */
	@Test
	public void testBasicQueue() throws Exception {

		CountDownLatch latch = new CountDownLatch(2);

		//生产者
		String hello = "hello";
		threadPool.submit(() -> {
			try {
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				MessageProducer producer = session.createProducer(testQueue);
				producer.send(session.createTextMessage(hello));
				session.close();
				latch.countDown();
			} catch (JMSException e) {
				e.printStackTrace();
			}


		});

		// 消费者
		threadPool.submit(() -> {
			try {
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				MessageConsumer consumer = session.createConsumer(testQueue);
				TextMessage message = (TextMessage) consumer.receive();
				session.close();
				logger.info(message);
				if (message.getText().equals(hello)) {
					latch.countDown();
				}


			} catch (JMSException e) {
				e.printStackTrace();
			}


		});
		latch.await(1, TimeUnit.MINUTES);
	}


	@Test
	public void testQueueOrder() throws Exception {

		CountDownLatch latch = new CountDownLatch(2);

		List<Integer> list = Arrays.asList(1, 2, 3);
		List<Integer> received = new ArrayList<>();


		//生产者
		threadPool.submit(() -> {
			try {
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				MessageProducer producer = session.createProducer(testQueue);
				for (Integer integer : list) {

					producer.send(session.createObjectMessage(integer));

					logger.info("send message ");
				}
				session.close();
				latch.countDown();

			} catch (JMSException e) {
				e.printStackTrace();
			}


		});

		// 消费者
		threadPool.submit(() -> {
			try {
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				MessageConsumer consumer = session.createConsumer(testQueue);

				while (true) {

					ObjectMessage message = (ObjectMessage) consumer.receive();
					if (message == null) {
						break;
					}
					received.add((Integer) message.getObject());
					if (received.size() == list.size()) {
						break;
					}
				}

				session.close();

				logger.info(String.join(",", received.stream().map(Object::toString).collect(Collectors.toList())));


				if (list.equals(received)) {

					latch.countDown();
				}


			} catch (JMSException e) {
				e.printStackTrace();
			}


		});

		latch.await(10, TimeUnit.SECONDS);


	}

	@Test
	public void testQueueOrderWithExclusiveConsumer() throws Exception {

		Semaphore semaphore = new Semaphore(0);

		CountDownLatch latch = new CountDownLatch(6);

		List<Integer> list = Arrays.asList(1, 2, 3);

		List<Integer> received = Collections.synchronizedList(new ArrayList<>());


		//生产者
		threadPool.submit(() -> {



			try {
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				MessageProducer producer = session.createProducer(exclusiveQueue);
				for (Integer integer : list) {

					producer.send(session.createObjectMessage(integer));

					logger.info(String.format("send message %d", integer));
					latch.countDown();
				}
				session.close();
				logger.info("done");

				semaphore.release(3);
			} catch (JMSException e) {
				e.printStackTrace();
			}


		});

		// 3个消费者

		for (int i = 0; i < 3; i++) {

			threadPool.submit(() -> {

				try {
					logger.info("wait producer done");
					semaphore.acquire();
					logger.info("producer done>>>>>>");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				try {
					Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
					MessageConsumer consumer = session.createConsumer(exclusiveQueue);

					logger.info("start consume message");
					ObjectMessage message = (ObjectMessage) consumer.receive();
					session.close();
					logger.info("consume message done");
					received.add((Integer) message.getObject());

					latch.countDown();


				} catch (JMSException e) {
					e.printStackTrace();
				}


			});
		}
		latch.await(10, TimeUnit.SECONDS);

		logger.info(String.join(",", received.stream().map(Object::toString).collect(Collectors.toList())));

		Assert.assertThat(list, Is.is(received));



	}


}
