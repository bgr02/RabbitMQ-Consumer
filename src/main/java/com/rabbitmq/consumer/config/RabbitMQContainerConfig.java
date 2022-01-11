package com.rabbitmq.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.Channel;
import com.rabbitmq.consumer.receiver.Receiver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Profile("container")
@Configuration
public class RabbitMQContainerConfig {

    static final String topicExchangeName = "ack.receiver.exchange";

    static final String queueName = "ack.receiver.queue";

    @Bean
    DirectExchange exchange() {
        return new DirectExchange(topicExchangeName);
    }

    @Bean
    Queue queue() {
        Map<String, Object> arguments = new HashMap<>();

        arguments.put("x-dead-letter-exchange", "x.dead.exchange");
        arguments.put("x-dead-letter-routing-key", "dlx.routing.key");

        return new Queue(queueName, false, false, false, arguments);
    }

    @Bean
    Binding binding(Queue queue, DirectExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("ack.receiver.routing.key");
    }

    //[SimpleMessageListenerContainer의 특징]
    //1. SimpleMessageListenerContainer는 Message Listener Container이며 AbstractMessageListenerContainer를 상속받고 있고
    //AbstractMessageListenerContainer를 상속 받음으로써 AcknowledgeMode, Requeue등의 설정을 할 수 있는 것입니다.
    //2. SimpleMessageListenerContainer는 @Bean으로 선언하여 여러개를 등록함으로써 복수의 Queue에 대해서 메시지를 수신받을 수 있는데
    //한가지 주의할점은 각각의 SimpleMessageListenerContainer의 메서드명이 달라야 한다는 점입니다.
    //메서드명이 같아도 파라미터 종류와 개수가 다르면 메서드 오버로드로 처리되어 메서드 선언은 가능하나 애플리케이션 동작시 SimpleMessageListenerContainer 빈이
    //등록될때 같은 이름을 가진 SimpleMessageListenerContainer이 여러개 있을시 마지막으로 등록되는 빈만 유효하게 되고 이전의 같은 이름을 가진 빈들은 등록이
    //되지않아 해당 빈에서 받아야할 Queue의 메시지를 받지 못하게 됩니다.
    
    //MessageListener를 정의하여 setMessageListener에 할당해서 사용하는 방식의 SimpleMessageListenerContainer를
    //Bean으로 등록해서 메시지를 전달받습니다.
    @Bean
    SimpleMessageListenerContainer simpleMessageListenerContainer(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();

        container.setConnectionFactory(connectionFactory);
        container.setMessageListener(onlyMessageListener()); //Message 정보 출력
        //container.setMessageListener(messageChannelListener()); //Message, Channel 정보 출력
        container.setQueueNames("ack.test.queue"); //spring.profiles.active가 container인 경우 ack.test.queue를 생성하는 로직이 없으므로
        //spring.profiles.active를 annotation 또는 factory로 설정하여 한번 생성한 뒤 다시 spring.profiles.active를 container로 설정하여 테스트해야 합니다.
        //이미 ack.test.queue가 존재하는 경우는 바로 spring.profiles.active를 container로 테스트하면 됩니다.
        container.setAcknowledgeMode(AcknowledgeMode.AUTO);
        container.setDefaultRequeueRejected(true);

        return container;
    }

    //SimpleMessageListenerContainer에 등록한 Queue의 Message를 반환받으려는 경우 MessageListener의 
    //onMessage를 재정의하여 사용하면 됩니다.
    @Bean
    MessageListener onlyMessageListener() {
        return new MessageListener() {
            public void onMessage(Message message) {
                log.info("<==================== Message Info: " + message);
            }
        };
    }

    //SimpleMessageListenerContainer에 등록한 Queue의 Message와 Channel을 반환받으려는 경우 ChannelAwareMessageListener의
    //onMessage를 재정의하여 사용하면 됩니다.
    @Bean
    MessageListener messageChannelListener() {
        return new ChannelAwareMessageListener() {
            public void onMessage(Message message, Channel channel) {
                log.info("<==================== Message Info: " + message);
                log.info("<==================== Channel Info: " + channel);
            }
        };
    }

    //MessageListenerAdapter를 정의하여 setMessageListener에 할당해서 사용하는 방식의 SimpleMessageListenerContainer를
    //Bean으로 등록해서 메시지를 전달받습니다.
    @Bean
    SimpleMessageListenerContainer simpleMessageListenerContainerUseReceiver(ConnectionFactory connectionFactory, MessageListenerAdapter messageListenerAdapter) {
        messageListenerAdapter.setMessageConverter(jsonMessageConverter()); //해당 설정 없을시 메시지 타입이 맞지 않는 오류가 발생합니다.

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();

        container.setConnectionFactory(connectionFactory);
        container.setMessageListener(messageListenerAdapter);
        container.setQueueNames(queueName);
        container.setAcknowledgeMode(AcknowledgeMode.AUTO);
        container.setDefaultRequeueRejected(true);

        return container;
    }

    //Receiver라는 이름으로 정의한 Bean의 receiveMessage 메서드를 통해 Message의 바디(메시지 내용)를 수신받습니다. 해당 방식은 Message, Channel과 같은 상세항 정보는
    //받을 수 없으며 만약 Message, Channel 정보가 필요한 경우 위에서 사용한것 처럼 MessageListener를 정의하여 setMessageListener에 할당해서 사용해야 합니다.
    @Bean
    MessageListenerAdapter messageListenerAdapter(Receiver receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        //LocalDateTime serializable을 위해
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
        objectMapper.registerModule(dateTimeModule());

        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    public JavaTimeModule dateTimeModule() {
        return new JavaTimeModule();
    }

}
