package com.curtisnewbie;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yongjie.zhuang
 */
@Component
public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private static final AtomicInteger counter = new AtomicInteger(0);

    @PostConstruct
    void init() {
        new Thread(() -> {
            int prev = 0;
            for (; ; ) {
                try {
                    Thread.sleep(1000); // sleep 1 sec
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                final int now = counter.get();
                int rate = now - prev;
                if (rate != 0)
                    logger.info("Consume msg rate: {}/sec", rate);
                prev = now;
            }
        }).start();
    }

    @RabbitListener(
            bindings = @QueueBinding(
                    key = "trtl-demo",
                    value = @Queue(name = "trtl-queue"),
                    exchange = @Exchange(name = "trtl-exg")
            ),
            ackMode = "AUTO"
    )
    public void handle(Channel channel, Message msg) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int curr = counter.incrementAndGet();
        logger.info("Received message {}, count: {}", new String(msg.getBody(), StandardCharsets.UTF_8), curr);
    }

}
