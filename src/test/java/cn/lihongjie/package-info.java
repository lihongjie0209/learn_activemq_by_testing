/**
 *
 * activemq 有两种模式
 *
 * 1. 服务器推送模式(默认模式)
 *
 * 服务器会在消息到达时一次性给客户端推送(batch), 如果不限制这个推送消息的数量, 可能会导致以下问题
 *     1. 无法负载均衡, 当服务器的推送大小为1000, 而你只有100个消息时, 无论你有多少消费者, 这100给消息只会推送给第一个消费者
 *     2. 当第一个消费者收到过多的消息时, 可能会耗尽资源
 *     3. 为了避免上述的问题, 当一个消息需要很长时间来消费的话, 那么最好把推送数量限制到1
 *
 * 推送模式下, 只有消费者消费了50%的推送, 服务器才会继续推送消息
 *
 *
 * 2. 客户端拉取模式(效率低)
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 * @author 982264618@qq.com
 */
package cn.lihongjie;