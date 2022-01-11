package com.rabbitmq.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Profile("factory")
@Configuration
public class RabbitMQFactoryConfig {

    static final String topicExchangeName = "ack.test.exchange";

    static final String queueName = "ack.test.queue";

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
        return BindingBuilder.bind(queue).to(exchange).with("ack.routing.key");
    }

    //RabbitListenerContainerFactory는 AcknowledgeMode, Requeue, MessageConverter 등의 설정들을 RabbitListenerContainerFactory에서
    //설정한 뒤 여러 @RabbitListener에서 containerFactory 속성에서 설정하여 사용할 수 있습니다. 만약 RabbitListenerContainerFactory로
    //등록된 Bean이 하나인 경우 별도로 containerFactory에 설정하지 않아도 자동으로 적용되며 여러개의 RabbitListenerContainerFactory가 있는경우
    //사용할 RabbitListenerContainerFactory의 메서드명을 @RabbitListener에서 containerFactory 속성에 설정해주면 RabbitListenerContainerFactory에서
    //설정한 AcknowledgeMode, Requeue, MessageConverter 등의 설정들이 @RabbitListener에 적용됩니다.
    @Bean
    public RabbitListenerContainerFactory<SimpleMessageListenerContainer> rabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {

        SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(simpleRabbitListenerContainerFactory, connectionFactory);

        simpleRabbitListenerContainerFactory.setAcknowledgeMode(AcknowledgeMode.AUTO);
        simpleRabbitListenerContainerFactory.setDefaultRequeueRejected(true);
        simpleRabbitListenerContainerFactory.setMessageConverter(jsonMessageConverter());
        simpleRabbitListenerContainerFactory.setAfterReceivePostProcessors(message -> { //메지시를 전송받은 후 실행될 로직을 정의할 수 있습니다.
            log.info("Check Message: " + message);

            return message;
        });

        return simpleRabbitListenerContainerFactory;
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
