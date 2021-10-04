package com.curtisnewbie;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yongjie.zhuang
 */
@Component
public class Producer {

    @Autowired
    private RabbitTemplate template;

    private static final Logger log = LoggerFactory.getLogger(Producer.class);
    private final AtomicLong atl = new AtomicLong(0);

    @PostConstruct
    public void init() {
        Random rand = new Random();

        new Thread(() -> {

            for (; ; ) {
                template.convertAndSend("trtl-exg", "trtl-demo", "blablabla", (msg) -> {
                    msg.getMessageProperties().setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
                    return msg;
                });
                atl.incrementAndGet();

                try {
                    int next = rand.nextInt(50) + 50;
                    Thread.sleep(next);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(() -> {

            long prev = 0;
            long curr;

            for (; ; ) {

                curr = atl.get();
                log.info("Publish rate: {}/s", curr - prev);
                prev = curr;

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }


}
