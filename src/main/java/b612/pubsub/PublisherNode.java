package b612.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;

import java.util.ArrayList;
import java.util.List;

public class PublisherNode {

    static class MessageReceiverExample implements MessageReceiver {
        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            System.out.println("PubNode: Recieved Message");
            System.out.println("Data: " + message.getData().toStringUtf8());
            consumer.ack();
        }
    }

    public static void main(String... args) throws Exception {

        String subscriptionId = "my-topic-resp";
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of("weighty-vertex-165601", subscriptionId);
        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, new MessageReceiverExample()).build();
            subscriber.startAsync().awaitRunning();
            System.out.println("PublisherNode: subscriber created");
        } catch (IllegalStateException e) {
            System.out.println("PublisherNode: Subscriber unexpectedly stopped: " + e);
        }

        System.out.println("PublisherNode: creating pub");
        String topicId = "my-topic";

        int messageCount = 10;
        ProjectTopicName topicName = ProjectTopicName.of("weighty-vertex-165601", topicId);
        Publisher publisher = null;
        List<ApiFuture<String>> futures = new ArrayList<>();

        try {
            publisher = Publisher.newBuilder(topicName).build();
            System.out.println("PublisherNode: created. sending data...");
            for (int i = 0; i < messageCount; i++) {
                String message = "message-" + i;

                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(data)
                    .build();

                ApiFuture<String> future = publisher.publish(pubsubMessage);
                futures.add(future);
                System.out.println("PublisherNode: sent data "+ i);
            }
        } finally {
            List<String> messageIds = ApiFutures.allAsList(futures).get();

            for (String messageId : messageIds) {
                System.out.println(messageId);
            }

            if (publisher != null) {
                publisher.shutdown();
                subscriber.awaitTerminated();
            }
        }
    }
}
