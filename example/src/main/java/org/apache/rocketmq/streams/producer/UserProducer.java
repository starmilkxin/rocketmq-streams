package org.apache.rocketmq.streams.producer;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.streams.examples.pojo.User;

import java.util.ArrayList;
import java.util.Random;

public class UserProducer {
    public static final String topic = "user";
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("Pro_Group");

        producer.setNamesrvAddr("localhost:9876");

        producer.start();

//        long time = 1672560000000L;
        long time = 1672568000000L;

        ArrayList<User> result = new ArrayList<>();
        for (int i = 2; i <= 3; i++) {
            User user = new User("小红" + i, i/2, time + 1000*i);
            System.out.println(user);
            result.add(user);
        }

        for (User user : result) {
            byte[] body = JSON.toJSONBytes(user);
            Message msg = new Message(topic, "", body);
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);

            Thread.sleep(1000);
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}

