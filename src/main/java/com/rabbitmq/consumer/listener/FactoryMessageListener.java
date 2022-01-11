package com.rabbitmq.consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Profile("factory")
@Slf4j
@Component
public class FactoryMessageListener {

    @RabbitListener(queues = "ack.test.queue")
    void receiveMessage(Message message) {
        log.info("<==================== Factory Config Test Receive Message" + message);
    }

}
