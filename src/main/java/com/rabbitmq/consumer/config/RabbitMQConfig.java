package com.rabbitmq.consumer.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.HashMap;
import java.util.Map;

@Profile("annotation")
@Configuration
public class RabbitMQConfig {

    //Exchange 이름
    static final String topicExchangeName = "ack.test.exchange";

    //Queue 이름
    static final String queueName = "ack.test.queue";

    //해당 설정은 없어도 동작에 문제는 없으나 Exchange가 없는 상태에서 자동으로 생성함과 동시에 응답을 받고자 하는경우 필요합니다.
    //Declare Exchange
    @Bean
    DirectExchange exchange() { //Exchange의 타입인 Direct, Topic, Headers, Fanout에 맟춰서 DirectExchange, TopicExchange, HeadersExchange, FanoutExchange가 있습니다.
        return new DirectExchange(topicExchangeName); //해당 Exchange가 없을시 생성하게 되며 존재하는 경우 존재하는 Exchange를 사용하는데
        //주의할 점이 있습니다. 같은 이름으로 Exchange가 존재하는 상태에서 Exchange 타입이 다르면 새로 생성도 못하고 찾지도 못하게 되어 에러가 발생합니다.
    }

    //해당 설정은 없어도 동작에 문제는 없으나 Queue가 없는 상태에서 자동으로 생성함과 동시에 응답을 받고자 하는경우 필요합니다.
    //Declare Queue
    @Bean
    Queue queue() {
        Map<String, Object> arguments = new HashMap<>();

        //[DLX 설정 방법]
        //DLX를 설장하는 방법은 Queue에 arguments를 설정하는 방식과 Policy를 생성하여 설정하는 방식이 있는데 여기서는 arguments를 설정하는 방식을 사용하겠습니다.
        //1. RabbitMQ 관리자 콘솔에서 DLX(Dead Letter Exchange)를 x.dead.exchange라는 이름으로 생성합니다. DLX라고 해서 특별한 Exchange가 아니고 그냥 평범한 Exchange입니다.
        //맡은 역할이 Dead Letter를 전송하는 Exchange라서 그렇게 부를뿐입니다. 타입은 Direct로 생성하겠습니다. 상황에 따라 다른 타입으로 생성해도 됩니다.
        //2. DLX에서 전달받은 메시지를 넘겨줄 Queue를 dead.letter.queue라는 이름으로 생성합니다.
        //3. x.dead.exchange와 dead.queue를 routing key로 바인딩합니다. routing key 값은 dlx.routing.key로 설정합니다.
        //4. DLX에서 전송받는 Dead Leter는 다른 exchange와 바인딩된 Queue에서 전달받는데 이 둘을 연결하는 역할을 하는것이 x-dead-letter-exchange, x-dead-letter-routing-key 두 가지
        //arguments입니다.
        //5. DLX에 Dead Leter를 전송해주는 다른 exchange와 바인딩된 Queue의 이름을 ack.test.queue로 설정하여 생성할때 arguments에 x-dead-letter-exchange는 x.dead.exchange로
        //x-dead-letter-routing-key는 dlx.routing.key로 설정하여 Queue를 생성합니다.
        //6. ack.test.queue에서 아래 3가지 상황이 발생하는 경우 DLX인 x.dead.exchange로 Dead Letter가 전송되고 x.dead.exchange는 dead.letter.queue로 Dead Letter를 보내줍니다.
        //[Dead Letter로 처리되는 메시지 상황]
        //1. basic.reject나 basic.nack 처리되는 경우
        //2. Queue에서 메시지 TTL이 다 된 경우(expire)
        //3. Queue가 가득차서 넘치는 경우(x-max-length)
        //7. dead.letter.queue에 Dead Letter가 쌓여도 자동으로 Dead Letter가 삭제되는 등의 처리가 되지는 않습니다. dead.letter.queue와 연결된 Consumer에서 Dead Letter를 가져가서
        //ack를 보내주고 Requeue 처리하는 등의 적절한 로직을 구성하여 Dead Letter를 처리해야 합니다.
        //8. DLX 설정을 위해서는 위의 설정을 마치가 한가지더 설정이 있습니다. 메시지를 전송받는 Listener(Consumer, 위 설명의 ack.test.queue의 Consumer)는 Auto ack와 Requeue가
        //false여야 한다는 점입니다. 이유는 Auto ack가 true인 경우에(spring.rabbitmq.listener.simple.acknowledge-mode가 NONE OR @RabbitListener의 ackMode가 NONE)
        //Listener에서 메시지를 받으면 무조건 ack를 자동으로 보내게 되고 메시지는 사라지게 됩니다. 따라서 DLX로 전송할 메시지가 없어지게 됩니다.
        //Requeue가 true인 경우 DLX로 메시지가 전송되지 않고 메시지를 보내준 Queue로 다시 돌아가게 되어 DLX로 메시지가 전송되지 못하게 됩니다.

        //Arguments를 통하여 DLX, TTL등의 설정들을 Queue에 적용합니다.
        //Queue는 DLX, TTL등의 설정을 Arguments 외에 설정하는 방법이 하나 더 있는데 Policy입니다. 우선순위로는 Policy보다 Arguments가 더 높습니다.
        arguments.put("x-dead-letter-exchange", "x.dead.exchange");
        arguments.put("x-dead-letter-routing-key", "dlx.routing.key");

        return new Queue(queueName, false, false, false, arguments); //해당 Queue가 없을시 생성하게 되며 존재하는 경우 존재하는 Queue를 사용하는데
        //주의할 점이 있습니다. 같은 이름으로 Queue가 존재하는 상태에서 Queue의 속성(durable, exclusive, autoDelete)이 하나라도 다르면 새로 생성도 못하고 찾지도 못하게 되어 에러가 발생합니다.
    }

    //해당 설정은 없어도 동작에 문제는 없으나 Exchange와 Queue의 binding이 없는 상태에서 자동으로 binding함과 동시에 응답을 받고자 하는경우 필요합니다.
    //Binding
    @Bean
    Binding binding(Queue queue, DirectExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("ack.routing.key");
    }

    //Message Listener Container
//    @Bean
//    SimpleMessageListenerContainer container(ConnectionFactory connectionFactory, MessageListenerAdapter listenerAdapter) {
//        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
//        container.setConnectionFactory(connectionFactory);
//        container.setQueueNames(queueName);
//        container.setMessageListener(listenerAdapter);
//
//        return container;
//    }
//
//    @Bean
//    MessageListenerAdapter listenerAdapter(Receiver receiver) {
//        return new MessageListenerAdapter(receiver, "receiveMessage");
//    }

//    @Bean
//    public RabbitListenerContainerFactory<SimpleMessageListenerContainer> prefetchOneContainerFactory(
//            SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {
//
//        SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory = new SimpleRabbitListenerContainerFactory();
//        configurer.configure(simpleRabbitListenerContainerFactory, connectionFactory);
//        simpleRabbitListenerContainerFactory.setPrefetchCount(1);
//
//        return simpleRabbitListenerContainerFactory;
//
//    }

}
