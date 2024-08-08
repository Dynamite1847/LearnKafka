package org.learnKafka.test.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdminTopicTest {
    public static void main(String[] args) {
        Map<String, Object> confMap = new HashMap<>();

        confMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //Create Admin
        final Admin admin = Admin.create(confMap);

        NewTopic topic1 = new NewTopic("test1", 1, (short) 1);
        NewTopic topic2 = new NewTopic("test2", 2, (short) 2);

        //create a topic with specific assign policy
        Map<Integer, List<Integer>> map = new HashMap<>();
        map.put(0, Arrays.asList(3,1));
        map.put(1, Arrays.asList(2,3));
        map.put(2, Arrays.asList(1,2));

        NewTopic topic3 = new NewTopic("test3", map);

        //create topics
        CreateTopicsResult  result = admin.createTopics(Arrays.asList(topic1, topic2,topic3));

        admin.close();
    }
}
