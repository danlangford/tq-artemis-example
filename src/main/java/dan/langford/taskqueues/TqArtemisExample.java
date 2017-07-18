/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dan.langford.taskqueues;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsQueue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.jms.JMSException;

import java.net.URI;

/**
 * A simple example that demonstrates server side load-balancing of messages between the queue instances on different
 * nodes of the cluster. The cluster is created from a static list of nodes.
 */
public class TqArtemisExample {


   public static void main(final String[] args) throws Exception {

      Connection connection0 = null;
      Connection connection1 = null;
      Connection connection2 = null;
      Connection connection3 = null;

      try {
         // Step 2. Use direct instantiation
         Queue queue = new JmsQueue("exampleQueue");

         // Step 3. new JMS Connection Factories
         ConnectionFactory cf0 = new JmsConnectionFactory("amqp://localhost:61616");
         ConnectionFactory cf1 = new JmsConnectionFactory("amqp://localhost:61617");
         ConnectionFactory cf2 = new JmsConnectionFactory("amqp://localhost:61618");
         ConnectionFactory cf3 = new JmsConnectionFactory("amqp://localhost:61619");

         Thread.sleep(2000);
         // Step 6. We create a JMS Connections to each of 4 servers
         connection0 = cf0.createConnection();
         connection1 = cf1.createConnection();
         connection2 = cf2.createConnection();
         connection3 = cf3.createConnection();

         // Step 8. We create a JMS Sessions on each server
         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session3 = connection3.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 10. We start the connections to ensure delivery occurs on them
         connection0.start();
         connection1.start();
         connection2.start();
         connection3.start();

         // Step 11. We create JMS MessageConsumer objects on server 0 server 1 server 2 server 3
         MessageConsumer consumer0 = session0.createConsumer(queue);
         MessageConsumer consumer1 = session1.createConsumer(queue);
         MessageConsumer consumer2 = session2.createConsumer(queue);
         MessageConsumer consumer3 = session3.createConsumer(queue);

         Thread.sleep(2000);

         // Step 12. We create a JMS MessageProducer object on server 3
         MessageProducer producer = session3.createProducer(queue);

         // Step 13. We send some messages to server 3

         final int numMessages = 20;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session0.createTextMessage("This is text message " + i);
            producer.send(message);
            System.out.println("Sent message: " + message.getText());
         }

         Thread.sleep(2000);

         // Step 14. We now consume those messages on  server 0 server 1 server 2 server 3
         // We note the messages have been distributed between servers in a round robin fashion
         // JMS Queues implement point-to-point message where each message is only ever consumed by a
         // maximum of one consumer
         int con0Node = getServer(connection0);
         int con1Node = getServer(connection1);
         int con2Node = getServer(connection2);
         int con3Node = getServer(connection3);

         System.out.println(String.format("con0Node:%d con1Node:%d con2Node:%d con3Node:%d", con0Node, con1Node, con2Node, con3Node));

         if (con0Node + con1Node + con2Node + con3Node != 6) {
            throw new IllegalStateException();
         }

         int i = 0;
         while ( i < numMessages) {

            i+=maybeMessage(consumer0, con0Node);
            i+=maybeMessage(consumer1, con1Node);
            i+=maybeMessage(consumer2, con2Node);
            i+=maybeMessage(consumer3, con3Node);

         }
      } finally {
         // Step 15. Be sure to close our resources!

         if (connection0 != null) {
            connection0.close();
         }

         if (connection1 != null) {
            connection1.close();
         }

         if (connection2 != null) {
            connection2.close();
         }

         if (connection3 != null) {
            connection3.close();
         }
      }
   }

   private static int maybeMessage(MessageConsumer consumer, int conNum) throws JMSException {
      TextMessage msg = (TextMessage) consumer.receive(1000);
      if (msg==null) {
         System.out.println("nothing for node " + conNum);
         return 0;
      } else {
         System.out.println("Got message: " + msg.getText() + " from node " + conNum);
         return 1;
      }
   }

   private static int getServer(Connection conn) {
         URI connUri = ((JmsConnection) conn).getConnectedURI();
         return connUri.getPort()-61616;
   }


}
