package b612.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;

public class SubscriberNode {
    private static Publisher publisher = null;

    static class MessageReceiverExample implements MessageReceiver {
        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            System.out.println("SubscriberNode: Recieved message");
            System.out.println("SubcriberNode: Data: " + message.getData().toStringUtf8());
            try {
                System.out.println("SubscriberNode: Doing the heavylifting of sleeping!");
                int random = (int)(Math.random() * 1000);
                Thread.sleep(random);
                System.out.println("SubscriberNode: Done with the heavylifting of sleeping!");

                acknowledge(message.getData());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            consumer.ack();
        }
    }

    public static void acknowledge(ByteString message) throws InterruptedException, ExecutionException {
        List<ApiFuture<String>> futures = new ArrayList<>();

        try {
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(message)
                .build();
            System.out.println("SubsciberNode: trying to ack message: "+ message);
            ApiFuture<String> future = SubscriberNode.publisher.publish(pubsubMessage);
            futures.add(future);
        } finally {
        	System.out.println("SubscriberNode: Hopefully acked");
        }
    }

    public static void main(String... args) throws Exception {
        String topicId = "my-topic-resp";
        ProjectTopicName topicName = ProjectTopicName.of("weighty-vertex-165601", topicId);

        SubscriberNode.publisher = Publisher.newBuilder(topicName).build();
        System.out.println("SubscriberNode: built publisher");

        String subscriptionId = "my-topic";
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of("weighty-vertex-165601", subscriptionId);
        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, new MessageReceiverExample()).build();
            subscriber.startAsync().awaitRunning();
            System.out.println("SubscriberNode: sub waiting for data");

            subscriber.awaitTerminated();
            System.out.println("SubscriberNode: sub waiting for termination");
        } catch (IllegalStateException e) {
            System.out.println("Subscriber unexpectedly stopped: " + e);
        }
    }
}
