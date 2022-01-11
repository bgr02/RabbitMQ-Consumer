package com.rabbitmq.consumer.listener;

import com.rabbitmq.client.Channel;
import com.rabbitmq.consumer.dto.MessageInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Profile("annotation")
@Slf4j
@Component
//[RabbitMQ 설정 우선순위]
//application.yml에서 설정하는 rabbitmq의 전역 설정보다 @RabbitListener의 속성에서 하는 설정이 우선 순위가 더 높습니다.

//[@RabbitListener 속성중 ackMode에 대한 설명(@RabbitListener에서 ackMode를 설정할시 spring.rabbitmq.listener.simple.acknowledge-mode 보다 우선순위가 높으므로
//ackMode에서 설정한 값이 적용됩니다.)]
//1. NONE: Consumer가 메시지를 받으면 자동으로 ack를 보냅니다.
//2. MANUAL: Consumer가 수동으로 ack 또는 nack를 보내주어야 합니다.
//3. AUTO: Consumer에서 에러가 발생하지 않는경우 ack를 에러가 발생하는 경우 nack를 보냅니다. (spring.rabbitmq.listener.simple.acknowledge-mode의 기본값은 AUTO입니다.)

//[@RabbitListener를 상세하게 설정하고 싶은경우 아래와 같이 사용합니다.]
//1. @RabbitListener(bindings = @QueueBinding(
//        value = @Queue(value = "auto.headers", autoDelete = "true",
//                arguments = @Argument(name = "x-message-ttl", value = "10000",
//                        type = "java.lang.Integer")),
//        exchange = @Exchange(value = "auto.headers", type = ExchangeTypes.HEADERS, autoDelete = "true"),
//        arguments = { //exchange의 arguments를 설정합니다.
//                @Argument(name = "x-match", value = "all"),
//                @Argument(name = "thing1", value = "somevalue"),
//                @Argument(name = "thing2")
//        })
//)
//2. @RabbitListener(bindings = @QueueBinding(
//        value = @Queue(value = "myQueue", durable = "true"),
//        exchange = @Exchange(value = "auto.exch", ignoreDeclarationExceptions = "true"),
//        key = "orderRoutingKey")
//)
//3. @RabbitListener(bindings = @QueueBinding(
//        value = @Queue,
//        exchange = @Exchange(value = "auto.exch"),
//        key = "invoiceRoutingKey")
//)
//4. @RabbitListener(queuesToDeclare = @Queue(name = "${my.queue}", durable = "true"))
//[@RabbitListener의 concurrency]
//concurrency를 설정하게 되면 queue에 연결되는 Consumer의 개수를 조절할 수 있습니다. default는 1로 하나의 Consumer만 연결되며
//만약 3으로 늘리게되면 queue에 3개의 Consumer가 생성되어 3개의 Consumer가 메시지를 가져가서 처리합니다.
public class AnnotationMessageListener {
    //ack.test.queue는 RabbitMQConfig에서 arguments에 DLX 설정이 되어있는데 현재 @RabbitListener에서 ackMode 설정, nack or reject 수행을
    //하고 있지 않기 때문에 arguments에 DLX 설정이 적용되지 않는 상태입니다. 만약 DLX 설정을 유효하게 하려면 ackMode를 MANUAL로 변경하고
    //channel.basicNack(requeue를 false로 설정) 또는 channel.basicReject(requeue를 false로 설정)를 수행해야 합니다.
    @RabbitListener(queues = "ack.test.queue", messageConverter = "jsonMessageConverter") //jsonMessageConverter를 Bean으로 동록하지 않을시 Producer에서 보낸 메시지
    //타입(Producer에서 사용하고 있는 Dto 객체, Consumer 측에도 해당 Dto 객체가 있어야 함)으로 메시지를 변환하는 기능이 수행되지 못하고 따라서 메시지를 받으려고 하면 메시지 타입 오류가
    //발생하게 됩니다. jsonMessageConverter라는 이름은 RabbitMQAnnotationConfig에 Bean으로 등록되어 있는 메서드명입니다. @Bean으로 jsonMessageConverter를 등록만 해놓으면
    //별도로 @RabbitListener의 속성으로 messageConverter를 설정하지 않아도 자동으로 사용이 가능하지만 위에서는 설명을 위해서 명시적으로 속성을 표시 해놓았습니다.
    void receiveMessage(Message message) {
        log.info("<==================== Receive Message" + message);
    }

    //@Exchange, @Queue에 선언된 Exchange, Queue가 없을시 자동으로 생성해주며 기존에 존재할시 모든 속성(Queue의 경우 arguments가 설정되어 있으므로 arguments 포함)이 같아야
    //정상적으로 연결됩니다.
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "messageInfo.test.queue", durable = "false", exclusive = "false", autoDelete = "false",
                    arguments = { @Argument(name = "x-dead-letter-exchange", value = "dead.letter.exchange", type = "java.lang.String"),
                            @Argument(name = "x-dead-letter-routing-key", value = "dlx.routing.key", type = "java.lang.String") }),
            exchange = @Exchange(value = "messageInfo.test.exchange", type = ExchangeTypes.DIRECT, autoDelete = "false"),
            key = "messageInfo.routing.key"),
            ackMode = "AUTO",
            messageConverter = "jsonMessageConverter"
    )
    void receiveMessageInfo(MessageInfo messageInfo) {
        log.info("<==================== Receive MessageInfo" + messageInfo);
    }

    //@Exchange, @Queue에 선언된 Exchange, Queue가 없을시 자동으로 생성해주며 기존에 존재할시 모든 속성(Queue의 경우 arguments가 설정되어 있으므로 arguments 포함)이 같아야
    //정상적으로 연결됩니다.
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "nack.test.queue", durable = "false", exclusive = "false", autoDelete = "false",
                    arguments = { @Argument(name = "x-dead-letter-exchange", value = "dead.letter.exchange", type = "java.lang.String"),
                            @Argument(name = "x-dead-letter-routing-key", value = "dlx.routing.key", type = "java.lang.String") }),
            exchange = @Exchange(value = "nack.test.exchange", type = ExchangeTypes.DIRECT, autoDelete = "false"),
            key = "nack.routing.key"),
            ackMode = "MANUAL",
            messageConverter = "jsonMessageConverter"
    )
    void nackMessage(Message message, Channel channel) throws IOException {
        log.info("<==================== Nack Message");
        channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
    }

    //@Exchange, @Queue에 선언된 Exchange, Queue가 없을시 자동으로 생성해주며 기존에 존재할시 모든 속성(Queue의 경우 arguments가 설정되어 있으므로 arguments 포함)이 같아야
    //정상적으로 연결됩니다.
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "reject.test.queue", durable = "false", exclusive = "false", autoDelete = "false",
                    arguments = { @Argument(name = "x-dead-letter-exchange", value = "dead.letter.exchange", type = "java.lang.String"),
                            @Argument(name = "x-dead-letter-routing-key", value = "dlx.routing.key", type = "java.lang.String") }),
            exchange = @Exchange(value = "reject.test.exchange", type = ExchangeTypes.DIRECT, autoDelete = "false"),
            key = "reject.routing.key"),
            ackMode = "MANUAL",
            messageConverter = "jsonMessageConverter"
    )
    void rejectMessage(Message message, Channel channel) throws IOException {
        log.info("<==================== Reject Message");
        channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
    }

    //@Exchange, @Queue에 선언된 Exchange, Queue가 없을시 자동으로 생성해주며 기존에 존재할시 모든 속성(Queue의 경우 arguments가 설정되어 있으므로 arguments 포함)이 같아야
    //정상적으로 연결됩니다.
    //해당 로직은 x-message-ttl를 설정하여 Queue에 저장된 메시지가 TTL(Time to live)에 설정된 시간이 지나면 DLX로 전송되는 로직을 구현한것입니다.
    //x-message-ttl 테스트는 다른 테스트와는 과정이 조금 다른데 TTL은 Queue에 Consumer가 없어서 메시지가 쌓여 Ready 상태인 메시지가 존재할때
    //설정한 TTL이 지나면 쌓인 메시지가 DLX로 이동하게 되므로 메시지의 Consumer가 없어야 하며 Consumer가 없으므로 ack, nack, reject 로직도 작성하지 않습니다.
    //단지, DLX와 TTL arguments를 설정한 Queue를 생성하고 해당 Queue에 Consumer가 없는 상태에서 메시지를 보내주기만 하면 됩니다.
    //해당 로직에서는 아래 로직의 주석을 풀어서 어플리케이션을 동작하여 Queue를 생성후 어플리케이션을 끄고 로직을 주석처리 하고 다시 어플리케이션을 동작시켜
    //Consumer가 없는 상태로 만드는 방법으로 테스트를 하였습니다.
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "ttl.test.queue", durable = "false", exclusive = "false", autoDelete = "false",
                    arguments = { @Argument(name = "x-dead-letter-exchange", value = "dead.letter.exchange", type = "java.lang.String"),
                            @Argument(name = "x-dead-letter-routing-key", value = "dlx.routing.key", type = "java.lang.String"),
                            @Argument(name = "x-message-ttl", value = "10000", type = "java.lang.Integer")}),
            exchange = @Exchange(value = "ttl.test.exchange", type = ExchangeTypes.DIRECT, autoDelete = "false"),
            key = "ttl.routing.key"),
            ackMode = "MANUAL",
            messageConverter = "jsonMessageConverter"
    )
    void ttlMessage(Message message, Channel channel) throws IOException {

    }

    //DLX를 통해 전달받은 Dead Letter를 저장하고 있는 dead.letter.queue로 부터 Dead Letter를 가져옵니다.
    //해당 로직을 주석 처리하는 경우 dead.letter.queue에 쌓인 메시지(Dead Letter)는 소비되지 않아 계속 쌓여있게 되고
    //해당 로직이 활성화 되어있는 경우 dead.letter.queue에서 메시지를 계속해서 가져오게 되므로 dead.letter.queue에
    //메시지가 쌓이지 않게 됩니다.
    @RabbitListener(queues = "dead.letter.queue", messageConverter = "jsonMessageConverter")
    void receiveDeadLetter(Message message) {
        log.info("<==================== Receive Dead Letter" + message);
    }

}
