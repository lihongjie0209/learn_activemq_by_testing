package cn.lihongjie.pubsub;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.*;
import org.junit.runner.Description;
import org.nutz.log.Log;

import javax.jms.*;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.nutz.log.Logs.get;

/**
 * @author 982264618@qq.com
 */
public class TopicTest {


	private static Log logger = get();
	private ActiveMQTopic topic;
	private static ExecutorService threadPool;


	volatile boolean producerDone = false;
	ReentrantLock lock = new ReentrantLock();
	Condition producerStatusChange = lock.newCondition();

	private static void logInfo(Description description, String status, long nanos) {
		String testName = description.getMethodName();
		logger.info(format("Test %s %s, spent %d microseconds",
				testName, status, NANOSECONDS.toMicros(nanos)));
	}

	@Rule
	public org.junit.rules.Stopwatch stopwatch = new org.junit.rules.Stopwatch() {
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


	@ClassRule
	public static EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

	private static ConnectionFactory connectionFactory;


	@BeforeClass
	public static void init() throws JMSException {
		broker.start();
		connectionFactory = new ActiveMQConnectionFactory(broker.getVmURL(false));

		threadPool = Executors.newCachedThreadPool();


	}


	@AfterClass
	public static void destroy() throws JMSException {

		threadPool.shutdown();

	}


	@Before
	public void setUp() throws Exception {
		topic = new ActiveMQTopic(UUID.randomUUID().toString());
	}


	/**
	 *
	 *
	 * 发布订阅模型当订阅者不在线时消息会丢失, 默认是不持久化的
	 * @throws Exception
	 */
	@Test
	public void testBasicPubSubWhenMessageIsPublishedAndSubIsOffline() throws Exception {

		final int subCount = 10;

		CountDownLatch latch = new CountDownLatch(subCount);


		for (int i = 0; i < subCount; i++) {


			// 订阅者
			threadPool.submit(() -> {

				logger.info("start consumer");
				lock.lock();
				while (!producerDone) {

					try {
						logger.info("consumer sleep");
						producerStatusChange.await();
						logger.info("consumer wakeup");
					} catch (InterruptedException e) {
						e.printStackTrace();
					} finally {
					}


				}
				lock.unlock();


				try {
					Connection connection = connectionFactory.createConnection();

					connection.start();

					Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

					MessageConsumer consumer = session.createConsumer(topic);

					ObjectMessage message = (ObjectMessage) consumer.receive();// block for message


					logger.info(String.format("收到消息: %s", message.getObject().toString()));
					session.close();

					connection.close();

					latch.countDown();
				} catch (JMSException e) {
					e.printStackTrace();
				}


			});


		}


		// 发布者
		threadPool.submit(() -> {

			try {
				Connection connection = connectionFactory.createConnection();

				connection.start();

				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

				MessageProducer producer = session.createProducer(topic);

				producer.send(session.createObjectMessage(1));


				session.close();

				connection.close();

				logger.info("producer done");
				lock.lock();
				logger.info("producer update status");
				producerDone = true;
				producerStatusChange.signalAll();
				logger.info("producer wakeup consumers");
				lock.unlock();


			} catch (JMSException e) {
				e.printStackTrace();
			}


		});


		latch.await(10, TimeUnit.SECONDS);

	}




	/**
	 * 发布订阅模型可以使用持久化订阅, 对于注册的持久化订阅者, 消息会保留在broker中直到被消费
	 *
	 * 持久化订阅者的id是由 : clientID + subscriberID 组成
	 *
	 * @throws Exception
	 */
	@Test
	public void testPersistentPubSub() throws Exception {

		final int subCount = 10;

		CountDownLatch subFinished = new CountDownLatch(subCount);
		CountDownLatch messageReceived = new CountDownLatch(subCount);

		// 先去订阅, 使用clientID作为唯一标识符

		Connection connection = connectionFactory.createConnection();
		connection.setClientID("test"); // 设置clientID
		connection.start();

		for (int i = 0; i < subCount; i++) {


			// 订阅者
			int finalI = i;
			threadPool.submit(() -> {

				logger.info("start consumer sub");

				try {

					Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
					session.createDurableSubscriber(topic, String.valueOf(finalI));  // 设置subscriberID
					session.close();
				} catch (JMSException e) {
					e.printStackTrace();
				}
				subFinished.countDown();
			});
		}


		// 发布者
		threadPool.submit(() -> {
			try {
				subFinished.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			try {

				connection.start();

				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

				MessageProducer producer = session.createProducer(topic);
				producer.send(session.createObjectMessage(1));
				session.close();
				logger.info("producer done");
				lock.lock();
				logger.info("producer update status");
				producerDone = true;
				producerStatusChange.signalAll();
				logger.info("producer wakeup consumers");
				lock.unlock();
			} catch (JMSException e) {
				e.printStackTrace();
			}


		});


		for (int i = 0; i < subCount; i++) {


			// 订阅者
			int finalI = i;
			threadPool.submit(() -> {

				logger.info("start consumer");
				lock.lock();
				while (!producerDone) {

					try {
						logger.info("consumer sleep");
						producerStatusChange.await();
						logger.info("consumer wakeup");
					} catch (InterruptedException e) {
						e.printStackTrace();
					} finally {
					}


				}
				lock.unlock();


				try {



					Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

					MessageConsumer consumer = session.createDurableSubscriber(topic, String.valueOf(finalI));

					ObjectMessage message = (ObjectMessage) consumer.receive();// block for message


					logger.info(String.format("收到消息: %s", message.getObject().toString()));
					session.close();


					messageReceived.countDown();
				} catch (JMSException e) {
					e.printStackTrace();
				}


			});


		}



		messageReceived.await(1, TimeUnit.MINUTES);

		connection.close();
	}














}
