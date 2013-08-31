/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DurableSubscriptionOfflineTest extends org.apache.activemq.TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionOfflineTest.class);
    public boolean usePrioritySupport = Boolean.TRUE;
    public int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    public boolean keepDurableSubsActive = true;
    private BrokerService broker;
    private ActiveMQTopic topic;
    private final List<Throwable> exceptions = new ArrayList<Throwable>();

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://" + getName(true));
        connectionFactory.setWatchTopicAdvisories(false);
        return connectionFactory;
    }

    @Override
    protected Connection createConnection() throws Exception {
        return createConnection("cliName");
    }

    protected Connection createConnection(String name) throws Exception {
        Connection con = super.createConnection();
        con.setClientID(name);
        con.start();
        return con;
    }

    public static Test suite() {
        return suite(DurableSubscriptionOfflineTest.class);
    }

    @Override
    protected void setUp() throws Exception {
        setAutoFail(true);
        setMaxTestTime(2 * 60 * 1000);
        exceptions.clear();
        topic = (ActiveMQTopic) createDestination();
        createBroker();
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        destroyBroker();
    }

    private void createBroker() throws Exception {
        createBroker(true);
    }

    private void createBroker(boolean deleteAllMessages) throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://" + getName(true) +")");
        broker.setBrokerName(getName(true));
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        broker.getManagementContext().setCreateConnector(false);
        broker.setAdvisorySupport(false);
        broker.setKeepDurableSubsActive(keepDurableSubsActive);
        broker.addConnector("tcp://0.0.0.0:0");

        if (usePrioritySupport) {
            PolicyEntry policy = new PolicyEntry();
            policy.setPrioritizedMessages(true);
            PolicyMap policyMap = new PolicyMap();
            policyMap.setDefaultEntry(policy);
            broker.setDestinationPolicy(policyMap);
        }

        setDefaultPersistenceAdapter(broker);
        if (broker.getPersistenceAdapter() instanceof JDBCPersistenceAdapter) {
            // ensure it kicks in during tests
            ((JDBCPersistenceAdapter)broker.getPersistenceAdapter()).setCleanupPeriod(2*1000);
        } else if (broker.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter) {
            // have lots of journal files
            ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).setJournalMaxFileLength(journalMaxFileLength);
        }
        broker.start();
        broker.waitUntilStarted();
    }

    private void destroyBroker() throws Exception {
        if (broker != null)
            broker.stop();
    }

    public void initCombosForTestConsumeOnlyMatchedMessages() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{ PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.LevelDB, PersistenceAdapterChoice.JDBC});
        this.addCombinationValues("usePrioritySupport",
                new Object[]{ Boolean.TRUE, Boolean.FALSE});
    }

    public void testConsumeOnlyMatchedMessages() throws Exception {
        // create durable subscription
        Connection con = createConnection();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            boolean filter = i % 2 == 1;
            if (filter)
                sent++;

            Message message = session.createMessage();
            message.setStringProperty("filter", filter ? "true" : "false");
            producer.send(topic, message);
        }

        session.close();
        con.close();

        // consume messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener = new Listener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals(sent, listener.count);
    }

     public void testConsumeAllMatchedMessages() throws Exception {
         // create durable subscription
         Connection con = createConnection();
         Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
         session.close();
         con.close();

         // send messages
         con = createConnection();
         session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(null);

         int sent = 0;
         for (int i = 0; i < 10; i++) {
             sent++;
             Message message = session.createMessage();
             message.setStringProperty("filter", "true");
             producer.send(topic, message);
         }

         Thread.sleep(1 * 1000);

         session.close();
         con.close();

         // consume messages
         con = createConnection();
         session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
         Listener listener = new Listener();
         consumer.setMessageListener(listener);

         Thread.sleep(3 * 1000);

         session.close();
         con.close();

         assertEquals(sent, listener.count);
     }

    public void initCombosForTestVerifyAllConsumedAreAcked() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
               new Object[]{ PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.LevelDB, PersistenceAdapterChoice.JDBC});
        this.addCombinationValues("usePrioritySupport",
                new Object[]{ Boolean.TRUE, Boolean.FALSE});
    }

     public void testVerifyAllConsumedAreAcked() throws Exception {
         // create durable subscription
         Connection con = createConnection();
         Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
         session.close();
         con.close();

         // send messages
         con = createConnection();
         session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(null);

         int sent = 0;
         for (int i = 0; i < 10; i++) {
             sent++;
             Message message = session.createMessage();
             message.setStringProperty("filter", "true");
             producer.send(topic, message);
         }

         Thread.sleep(1 * 1000);

         session.close();
         con.close();

         // consume messages
         con = createConnection();
         session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
         Listener listener = new Listener();
         consumer.setMessageListener(listener);

         Thread.sleep(3 * 1000);

         session.close();
         con.close();

         LOG.info("Consumed: " + listener.count);
         assertEquals(sent, listener.count);

         // consume messages again, should not get any
         con = createConnection();
         session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
         listener = new Listener();
         consumer.setMessageListener(listener);

         Thread.sleep(3 * 1000);

         session.close();
         con.close();

         assertEquals(0, listener.count);
     }

    public void testTwoOfflineSubscriptionCanConsume() throws Exception {
        // create durable subscription 1
        Connection con = createConnection("cliId1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // create durable subscription 2
        Connection con2 = createConnection("cliId2");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener2 = new Listener();
        consumer2.setMessageListener(listener2);

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // test online subs
        Thread.sleep(3 * 1000);
        session2.close();
        con2.close();

        assertEquals(sent, listener2.count);

        // consume messages
        con = createConnection("cliId1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener = new Listener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals("offline consumer got all", sent, listener.count);
    }

    public void initCombosForTestJMXCountersWithOfflineSubs() throws Exception {
        this.addCombinationValues("keepDurableSubsActive",
                new Object[]{Boolean.TRUE, Boolean.FALSE});
    }

    public void testJMXCountersWithOfflineSubs() throws Exception {
        // create durable subscription 1
        Connection con = createConnection("cliId1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", null, true);
        session.close();
        con.close();

        // restart broker
        broker.stop();
        createBroker(false /*deleteAllMessages*/);

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            producer.send(topic, message);
        }
        session.close();
        con.close();

        // consume some messages
        con = createConnection("cliId1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);

        for (int i=0; i<sent/2; i++) {
            Message m =  consumer.receive(4000);
            assertNotNull("got message: " + i, m);
            LOG.info("Got :" + i + ", " + m);
        }

        // check some counters while active
        ObjectName activeDurableSubName = broker.getAdminView().getDurableTopicSubscribers()[0];
        LOG.info("active durable sub name: " + activeDurableSubName);
        final DurableSubscriptionViewMBean durableSubscriptionView = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(activeDurableSubName, DurableSubscriptionViewMBean.class, true);

        assertTrue("is active", durableSubscriptionView.isActive());
        assertEquals("all enqueued", keepDurableSubsActive ? 10 : 0, durableSubscriptionView.getEnqueueCounter());
        assertTrue("correct waiting acks", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 5 == durableSubscriptionView.getMessageCountAwaitingAcknowledge();
            }
        }));
        assertEquals("correct dequeue", 5, durableSubscriptionView.getDequeueCounter());


        ObjectName destinationName = broker.getAdminView().getTopics()[0];
        TopicViewMBean topicView = (TopicViewMBean) broker.getManagementContext().newProxyInstance(destinationName, TopicViewMBean.class, true);
        assertEquals("correct enqueue", 10, topicView.getEnqueueCount());
        assertEquals("still zero dequeue, we don't decrement on each sub ack to stop exceeding the enqueue count with multiple subs", 0, topicView.getDequeueCount());
        assertEquals("inflight", 5, topicView.getInFlightCount());

        session.close();
        con.close();

        // check some counters when inactive
        ObjectName inActiveDurableSubName = broker.getAdminView().getInactiveDurableTopicSubscribers()[0];
        LOG.info("inactive durable sub name: " + inActiveDurableSubName);
        DurableSubscriptionViewMBean durableSubscriptionView1 = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(inActiveDurableSubName, DurableSubscriptionViewMBean.class, true);

        assertTrue("is not active", !durableSubscriptionView1.isActive());
        assertEquals("all enqueued", keepDurableSubsActive ? 10 : 0, durableSubscriptionView1.getEnqueueCounter());
        assertEquals("correct awaiting ack", 0, durableSubscriptionView1.getMessageCountAwaitingAcknowledge());
        assertEquals("correct dequeue", keepDurableSubsActive ? 5 : 0, durableSubscriptionView1.getDequeueCounter());

        // destination view
        assertEquals("correct enqueue", 10, topicView.getEnqueueCount());
        assertEquals("still zero dequeue, we don't decrement on each sub ack to stop exceeding the enqueue count with multiple subs", 0, topicView.getDequeueCount());
        assertEquals("inflight back to 0 after deactivate", 0, topicView.getInFlightCount());

        // consume the rest
        con = createConnection("cliId1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(topic, "SubsId", null, true);

        for (int i=0; i<sent/2;i++) {
            Message m =  consumer.receive(30000);
            assertNotNull("got message: " + i, m);
            LOG.info("Got :" + i + ", " + m);
        }

        activeDurableSubName = broker.getAdminView().getDurableTopicSubscribers()[0];
        LOG.info("durable sub name: " + activeDurableSubName);
        final DurableSubscriptionViewMBean durableSubscriptionView2 = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(activeDurableSubName, DurableSubscriptionViewMBean.class, true);

        assertTrue("is active", durableSubscriptionView2.isActive());
        assertEquals("all enqueued", keepDurableSubsActive ? 10 : 0, durableSubscriptionView2.getEnqueueCounter());
        assertTrue("correct dequeue", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                long val = durableSubscriptionView2.getDequeueCounter();
                LOG.info("dequeue count:" + val);
                return 10 == val;
            }
        }));
    }

    public void initCombosForTestOfflineSubscriptionCanConsumeAfterOnlineSubs() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{ PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.LevelDB, PersistenceAdapterChoice.JDBC});
        this.addCombinationValues("usePrioritySupport",
                new Object[]{ Boolean.TRUE, Boolean.FALSE});
    }

    public void testOfflineSubscriptionCanConsumeAfterOnlineSubs() throws Exception {
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        Connection con2 = createConnection("onlineCli1");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener2 = new Listener();
        consumer2.setMessageListener(listener2);

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // test online subs
        Thread.sleep(3 * 1000);
        session2.close();
        con2.close();
        assertEquals(sent, listener2.count);

        // restart broker
        broker.stop();
        createBroker(false /*deleteAllMessages*/);

        // test offline
        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);

        Connection con3 = createConnection("offCli2");
        Session session3 = con3.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer3 = session3.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);

        Listener listener = new Listener();
        consumer.setMessageListener(listener);
        Listener listener3 = new Listener();
        consumer3.setMessageListener(listener3);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();
        session3.close();
        con3.close();

        assertEquals(sent, listener.count);
        assertEquals(sent, listener3.count);
    }

    public void initCombosForTestInterleavedOfflineSubscriptionCanConsume() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.LevelDB, PersistenceAdapterChoice.JDBC});
    }

    public void testInterleavedOfflineSubscriptionCanConsume() throws Exception {
        // create durable subscription 1
        Connection con = createConnection("cliId1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);

        // create durable subscription 2
        Connection con2 = createConnection("cliId2");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener2 = new Listener();
        consumer2.setMessageListener(listener2);

        assertEquals(0, listener2.count);
        session2.close();
        con2.close();

        // send some more
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        con2 = createConnection("cliId2");
        session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer2 = session2.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        listener2 = new Listener("cliId2");
        consumer2.setMessageListener(listener2);
        // test online subs
        Thread.sleep(3 * 1000);

        assertEquals(10, listener2.count);

        // consume all messages
        con = createConnection("cliId1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener = new Listener("cliId1");
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals("offline consumer got all", sent, listener.count);
    }

    public void initCombosForTestMixOfOnLineAndOfflineSubsGetAllMatched() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{ PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.LevelDB, PersistenceAdapterChoice.JDBC});
    }

    private static String filter = "$a='A1' AND (($b=true AND $c=true) OR ($d='D1' OR $d='D2'))";
    public void testMixOfOnLineAndOfflineSubsGetAllMatched() throws Exception {
        // create offline subs 1
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        // create offline subs 2
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        // create online subs
        Connection con2 = createConnection("onlineCli1");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createDurableSubscriber(topic, "SubsId", filter, true);
        Listener listener2 = new Listener();
        consumer2.setMessageListener(listener2);

        // create non-durable consumer
        Connection con4 = createConnection("nondurableCli");
        Session session4 = con4.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer4 = session4.createConsumer(topic, filter, true);
        Listener listener4 = new Listener();
        consumer4.setMessageListener(listener4);

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        boolean hasRelevant = false;
        int filtered = 0;
        for (int i = 0; i < 100; i++) {
            int postf = (int) (Math.random() * 9) + 1;
            String d = "D" + postf;

            if ("D1".equals(d) || "D2".equals(d)) {
                hasRelevant = true;
                filtered++;
            }

            Message message = session.createMessage();
            message.setStringProperty("$a", "A1");
            message.setStringProperty("$d", d);
            producer.send(topic, message);
        }

        Message message = session.createMessage();
        message.setStringProperty("$a", "A1");
        message.setBooleanProperty("$b", true);
        message.setBooleanProperty("$c", hasRelevant);
        producer.send(topic, message);

        if (hasRelevant)
            filtered++;

        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        Thread.sleep(3 * 1000);

        // test non-durable consumer
        session4.close();
        con4.close();
        assertEquals(filtered, listener4.count); // succeeded!

        // test online subs
        session2.close();
        con2.close();
        assertEquals(filtered, listener2.count); // succeeded!

        // test offline 1
        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        Listener listener = new FilterCheckListener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);
        session.close();
        con.close();

        assertEquals(filtered, listener.count);

        // test offline 2
        Connection con3 = createConnection("offCli2");
        Session session3 = con3.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer3 = session3.createDurableSubscriber(topic, "SubsId", filter, true);
        Listener listener3 = new FilterCheckListener();
        consumer3.setMessageListener(listener3);

        Thread.sleep(3 * 1000);
        session3.close();
        con3.close();

        assertEquals(filtered, listener3.count);
        assertTrue("no unexpected exceptions: " + exceptions, exceptions.isEmpty());
    }

    public void testRemovedDurableSubDeletes() throws Exception {
        // create durable subscription 1
        Connection con = createConnection("cliId1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        for (int i = 0; i < 10; i++) {
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);

        Connection con2 = createConnection("cliId1");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session2.unsubscribe("SubsId");
        session2.close();
        con2.close();

        // see if retroactive can consumer any
        topic = new ActiveMQTopic(topic.getPhysicalName() + "?consumer.retroactive=true");
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        Listener listener = new Listener();
        consumer.setMessageListener(listener);
        session.close();
        con.close();
        assertEquals(0, listener.count);
    }

    public void testRemovedDurableSubDeletesFromIndex() throws Exception {

        if (! (broker.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter)) {
            return;
        }

        final int numMessages = 2750;

        KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter)broker.getPersistenceAdapter();
        PageFile pageFile = kahaDBPersistenceAdapter.getStore().getPageFile();
        LOG.info("PageCount " + pageFile.getPageCount() + " f:" + pageFile.getFreePageCount() + ", fileSize:" + pageFile.getFile().length());

        long lastDiff = 0;
        for (int repeats=0; repeats<2; repeats++) {

            LOG.info("Iteration: "+ repeats  + " Count:" + pageFile.getPageCount() + " f:" + pageFile.getFreePageCount());

            Connection con = createConnection("cliId1" + "-" + repeats);
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
            session.close();
            con.close();

            // send messages
            con = createConnection();
            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(null);

            for (int i = 0; i < numMessages; i++) {
                Message message = session.createMessage();
                message.setStringProperty("filter", "true");
                producer.send(topic, message);
            }
            con.close();

            Connection con2 = createConnection("cliId1" + "-" + repeats);
            Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session2.unsubscribe("SubsId");
            session2.close();
            con2.close();

            LOG.info("PageCount " + pageFile.getPageCount() + " f:" + pageFile.getFreePageCount() +  " diff: " + (pageFile.getPageCount() - pageFile.getFreePageCount()) + " fileSize:" + pageFile.getFile().length());

            if (lastDiff != 0) {
                assertEquals("Only use X pages per iteration: " + repeats, lastDiff, pageFile.getPageCount() - pageFile.getFreePageCount());
            }
            lastDiff = pageFile.getPageCount() - pageFile.getFreePageCount();
        }
    }

    public void initCombosForTestOfflineSubscriptionWithSelectorAfterRestart() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{ PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.LevelDB, PersistenceAdapterChoice.JDBC});
    }

    public void testOfflineSubscriptionWithSelectorAfterRestart() throws Exception {

        if (PersistenceAdapterChoice.LevelDB == defaultPersistenceAdapter) {
            // https://issues.apache.org/jira/browse/AMQ-4296
            return;
        }

        // create offline subs 1
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // create offline subs 2
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int filtered = 0;
        for (int i = 0; i < 10; i++) {
            boolean filter = (int) (Math.random() * 2) >= 1;
            if (filter)
                filtered++;

            Message message = session.createMessage();
            message.setStringProperty("filter", filter ? "true" : "false");
            producer.send(topic, message);
        }

        LOG.info("sent: " + filtered);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // restart broker
        Thread.sleep(3 * 1000);
        broker.stop();
        createBroker(false /*deleteAllMessages*/);

        // send more messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);

        for (int i = 0; i < 10; i++) {
            boolean filter = (int) (Math.random() * 2) >= 1;
            if (filter)
                filtered++;

            Message message = session.createMessage();
            message.setStringProperty("filter", filter ? "true" : "false");
            producer.send(topic, message);
        }

        LOG.info("after restart, total sent with filter='true': " + filtered);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // test offline subs
        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener = new Listener("1>");
        consumer.setMessageListener(listener);

        Connection con3 = createConnection("offCli2");
        Session session3 = con3.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer3 = session3.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener3 = new Listener();
        consumer3.setMessageListener(listener3);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();
        session3.close();
        con3.close();

        assertEquals(filtered, listener.count);
        assertEquals(filtered, listener3.count);
    }

    public void initCombosForTestOfflineSubscriptionAfterRestart() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{ PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.LevelDB, PersistenceAdapterChoice.JDBC});
    }

    public void testOfflineSubscriptionAfterRestart() throws Exception {
        // create offline subs 1
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, false);
        Listener listener = new Listener();
        consumer.setMessageListener(listener);

        // send messages
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "false");
            producer.send(topic, message);
        }

        LOG.info("sent: " + sent);
        Thread.sleep(5 * 1000);
        session.close();
        con.close();

        assertEquals(sent, listener.count);

        // restart broker
        Thread.sleep(3 * 1000);
        broker.stop();
        createBroker(false /*deleteAllMessages*/);

        // send more messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);

        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "false");
            producer.send(topic, message);
        }

        LOG.info("after restart, sent: " + sent);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // test offline subs
        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals(sent, listener.count);
    }

    public void testInterleavedOfflineSubscriptionCanConsumeAfterUnsub() throws Exception {
        // create offline subs 1
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // create offline subs 2
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", null, true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            boolean filter = (int) (Math.random() * 2) >= 1;

            sent++;

            Message message = session.createMessage();
            message.setStringProperty("filter", filter ? "true" : "false");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);

        Connection con2 = createConnection("offCli1");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session2.unsubscribe("SubsId");
        session2.close();
        con2.close();

        // consume all messages
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
        Listener listener = new Listener("SubsId");
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals("offline consumer got all", sent, listener.count);
    }

    public void testNoDuplicateOnConcurrentSendTranCommitAndActivate() throws Exception {
        final int messageCount = 1000;
        Connection con = null;
        Session session = null;
        final int numConsumers = 10;
        for (int i = 0; i <= numConsumers; i++) {
            con = createConnection("cli" + i);
            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId", null, true);
            session.close();
            con.close();
        }

        class CheckForDupsClient implements Runnable {
            HashSet<Long> ids = new HashSet<Long>();
            final int id;

            public CheckForDupsClient(int id) {
                this.id = id;
            }

            @Override
            public void run() {
                try {
                    Connection con = createConnection("cli" + id);
                    Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    for (int j=0;j<2;j++) {
                        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
                        for (int i = 0; i < messageCount/2; i++) {
                            Message message = consumer.receive(4000);
                            assertNotNull(message);
                            long producerSequenceId = new MessageId(message.getJMSMessageID()).getProducerSequenceId();
                            assertTrue("ID=" + id + " not a duplicate: " + producerSequenceId, ids.add(producerSequenceId));
                        }
                        consumer.close();
                    }

                    // verify no duplicates left
                    MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
                    Message message = consumer.receive(4000);
                    if (message != null) {
                        long producerSequenceId = new MessageId(message.getJMSMessageID()).getProducerSequenceId();
                        assertTrue("ID=" + id + " not a duplicate: " + producerSequenceId, ids.add(producerSequenceId));
                    }
                    assertNull(message);


                    session.close();
                    con.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                    exceptions.add(e);
                }
            }
        }

        final String payLoad = new String(new byte[1000]);
        con = createConnection();
        final Session sendSession = con.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = sendSession.createProducer(topic);
        for (int i = 0; i < messageCount; i++) {
            producer.send(sendSession.createTextMessage(payLoad));
        }

        ExecutorService executorService = Executors.newCachedThreadPool();

        // concurrent commit and activate
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sendSession.commit();
                } catch (JMSException e) {
                    e.printStackTrace();
                    exceptions.add(e);
                }
            }
        });
        for (int i = 0; i < numConsumers; i++) {
            executorService.execute(new CheckForDupsClient(i));
        }

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);
        con.close();

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }

    public void testOrderOnActivateDeactivate() throws Exception {
        for (int i=0;i<10;i++) {
            LOG.info("Iteration: " + i);
            doTestOrderOnActivateDeactivate();
            broker.stop();
            broker.waitUntilStopped();
            createBroker(true /*deleteAllMessages*/);
        }
    }

    public void doTestOrderOnActivateDeactivate() throws Exception {
        final int messageCount = 1000;
        Connection con = null;
        Session session = null;
        final int numConsumers = 4;
        for (int i = 0; i <= numConsumers; i++) {
            con = createConnection("cli" + i);
            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId", null, true);
            session.close();
            con.close();
        }

        final String url = "failover:(tcp://localhost:"
            + (broker.getTransportConnectors().get(1).getConnectUri()).getPort()
            + "?wireFormat.maxInactivityDuration=0)?"
            + "jms.watchTopicAdvisories=false&"
            + "jms.alwaysSyncSend=true&jms.dispatchAsync=true&"
            + "jms.sendAcksAsync=true&"
            + "initialReconnectDelay=100&maxReconnectDelay=30000&"
            + "useExponentialBackOff=true";
        final ActiveMQConnectionFactory clientFactory = new ActiveMQConnectionFactory(url);

        class CheckOrderClient implements Runnable {
            final int id;
            int runCount = 0;

            public CheckOrderClient(int id) {
                this.id = id;
            }

            @Override
            public void run() {
                try {
                    synchronized (this) {
                        Connection con = clientFactory.createConnection();
                        con.setClientID("cli" + id);
                        con.start();
                        Session session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
                        int nextId = 0;

                        ++runCount;
                        int i=0;
                        for (; i < messageCount/2; i++) {
                            Message message = consumer.receiveNoWait();
                            if (message == null) {
                                break;
                            }
                            long producerSequenceId = new MessageId(message.getJMSMessageID()).getProducerSequenceId();
                            assertEquals(id + " expected order: runCount: " + runCount  + " id: " + message.getJMSMessageID(), ++nextId, producerSequenceId);
                        }
                        LOG.info(con.getClientID() + " peeked " + i);
                        session.close();
                        con.close();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    exceptions.add(e);
                }
            }
        }

        Runnable producer = new Runnable() {
            final String payLoad = new String(new byte[600]);

            @Override
            public void run() {
                try {
                    Connection con = createConnection();
                    final Session sendSession = con.createSession(true, Session.SESSION_TRANSACTED);
                    MessageProducer producer = sendSession.createProducer(topic);
                    for (int i = 0; i < messageCount; i++) {
                        producer.send(sendSession.createTextMessage(payLoad));
                    }
                    LOG.info("About to commit: " + messageCount);
                    sendSession.commit();
                    LOG.info("committed: " + messageCount);
                    con.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptions.add(e);
                }
            }
        };

        ExecutorService executorService = Executors.newCachedThreadPool();

        // concurrent commit and activate
        for (int i = 0; i < numConsumers; i++) {
            final CheckOrderClient client = new CheckOrderClient(i);
            for (int j=0; j<100; j++) {
                executorService.execute(client);
            }
        }
        executorService.execute(producer);

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);
        con.close();

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }

    public void testUnmatchedSubUnsubscribeDeletesAll() throws Exception {
        // create offline subs 1
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int filtered = 0;
        for (int i = 0; i < 10; i++) {
            boolean filter = (i %2 == 0); //(int) (Math.random() * 2) >= 1;
            if (filter)
                filtered++;

            Message message = session.createMessage();
            message.setStringProperty("filter", filter ? "true" : "false");
            producer.send(topic, message);
        }

        LOG.info("sent: " + filtered);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // test offline subs
        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.unsubscribe("SubsId");
        session.close();
        con.close();

        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener = new Listener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals(0, listener.count);
    }

    public void testAllConsumed() throws Exception {
        final String filter = "filter = 'true'";
        Connection con = createConnection("cli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
            sent++;
        }

        LOG.info("sent: " + sent);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        con = createConnection("cli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        Listener listener = new Listener();
        consumer.setMessageListener(listener);
        Thread.sleep(3 * 1000);
        session.close();
        con.close();

        assertEquals(sent, listener.count);

        LOG.info("cli2 pull 2");
        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        assertNotNull("got message", consumer.receive(2000));
        assertNotNull("got message", consumer.receive(2000));
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);

        sent = 0;
        for (int i = 0; i < 2; i++) {
            Message message = session.createMessage();
            message.setStringProperty("filter", i==1 ? "true" : "false");
            producer.send(topic, message);
            sent++;
        }
        LOG.info("sent: " + sent);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        LOG.info("cli1 again, should get 1 new ones");
        con = createConnection("cli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        listener = new Listener();
        consumer.setMessageListener(listener);
        Thread.sleep(3 * 1000);
        session.close();
        con.close();

        assertEquals(1, listener.count);
    }

    // https://issues.apache.org/jira/browse/AMQ-3190
    public void testNoMissOnMatchingSubAfterRestart() throws Exception {

        final String filter = "filter = 'true'";
        Connection con = createConnection("cli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        // send unmatched messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        // message for cli1 to keep it interested
        Message message = session.createMessage();
        message.setStringProperty("filter", "true");
        message.setIntProperty("ID", 0);
        producer.send(topic, message);
        sent++;

        for (int i = sent; i < 10; i++) {
            message = session.createMessage();
            message.setStringProperty("filter", "false");
            message.setIntProperty("ID", i);
            producer.send(topic, message);
            sent++;
        }
        con.close();
        LOG.info("sent: " + sent);

        // new sub at id 10
        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        destroyBroker();
        createBroker(false);

        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);

        for (int i = sent; i < 30; i++) {
            message = session.createMessage();
            message.setStringProperty("filter", "true");
            message.setIntProperty("ID", i);
            producer.send(topic, message);
            sent++;
        }
        con.close();
        LOG.info("sent: " + sent);

        // pick up the first of the next twenty messages
        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        Message m = consumer.receive(3000);
        assertEquals("is message 10", 10, m.getIntProperty("ID"));

        session.close();
        con.close();

        // pick up the first few messages for client1
        con = createConnection("cli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        m = consumer.receive(3000);
        assertEquals("is message 0", 0, m.getIntProperty("ID"));
        m = consumer.receive(3000);
        assertEquals("is message 10", 10, m.getIntProperty("ID"));

        session.close();
        con.close();
    }

    // use very small journal to get lots of files to cleanup
    public void initCombosForTestCleanupDeletedSubAfterRestart() throws Exception {
        this.addCombinationValues("journalMaxFileLength",
                new Object[]{new Integer(64 * 1024)});
        this.addCombinationValues("keepDurableSubsActive",
                new Object[]{Boolean.TRUE, Boolean.FALSE});
    }

    // https://issues.apache.org/jira/browse/AMQ-3206
    public void testCleanupDeletedSubAfterRestart() throws Exception {
        Connection con = createConnection("cli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", null, true);
        session.close();
        con.close();

        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", null, true);
        session.close();
        con.close();

        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        final int toSend = 500;
        final String payload = new byte[40*1024].toString();
        int sent = 0;
        for (int i = sent; i < toSend; i++) {
            Message message = session.createTextMessage(payload);
            message.setStringProperty("filter", "false");
            message.setIntProperty("ID", i);
            producer.send(topic, message);
            sent++;
        }
        con.close();
        LOG.info("sent: " + sent);

        // kill off cli1
        con = createConnection("cli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.unsubscribe("SubsId");

        destroyBroker();
        createBroker(false);

        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
        final Listener listener = new Listener();
        consumer.setMessageListener(listener);
        assertTrue("got all sent", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Want: " + toSend  + ", current: " + listener.count);
                return listener.count == toSend;
            }
        }));
        session.close();
        con.close();

        destroyBroker();
        createBroker(false);
        final KahaDBPersistenceAdapter pa = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        assertTrue("Should have less than three journal files left but was: " +
            pa.getStore().getJournal().getFileMap().size(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return pa.getStore().getJournal().getFileMap().size() <= 3;
            }
        }));
    }

//    // https://issues.apache.org/jira/browse/AMQ-3768
//    public void testPageReuse() throws Exception {
//        Connection con = null;
//        Session session = null;
//
//        final int numConsumers = 115;
//        for (int i=0; i<=numConsumers;i++) {
//            con = createConnection("cli" + i);
//            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            session.createDurableSubscriber(topic, "SubsId", null, true);
//            session.close();
//            con.close();
//        }
//
//        // populate ack locations
//        con = createConnection();
//        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//        MessageProducer producer = session.createProducer(null);
//        Message message = session.createTextMessage(new byte[10].toString());
//        producer.send(topic, message);
//        con.close();
//
//        // we have a split, remove all but the last so that
//        // the head pageid changes in the acklocations listindex
//        for (int i=0; i<=numConsumers -1; i++) {
//            con = createConnection("cli" + i);
//            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            session.unsubscribe("SubsId");
//            session.close();
//            con.close();
//        }
//
//        destroyBroker();
//        createBroker(false);
//
//        // create a bunch more subs to reuse the freed page and get us in a knot
//        for (int i=1; i<=numConsumers;i++) {
//            con = createConnection("cli" + i);
//            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            session.createDurableSubscriber(topic, "SubsId", filter, true);
//            session.close();
//            con.close();
//        }
//    }
//
//    public void testRedeliveryFlag() throws Exception {
//
//        Connection con;
//        Session session;
//        final int numClients = 2;
//        for (int i=0; i<numClients; i++) {
//            con = createConnection("cliId" + i);
//            session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//            session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//            session.close();
//            con.close();
//        }
//
//        final Random random = new Random();
//
//        // send messages
//        con = createConnection();
//        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//        MessageProducer producer = session.createProducer(null);
//
//        final int count = 1000;
//        for (int i = 0; i < count; i++) {
//            Message message = session.createMessage();
//            message.setStringProperty("filter", "true");
//            producer.send(topic, message);
//        }
//        session.close();
//        con.close();
//
//        class Client implements Runnable {
//            Connection con;
//            Session session;
//            String clientId;
//            Client(String id) {
//                this.clientId = id;
//            }
//
//            @Override
//            public void run() {
//                MessageConsumer consumer = null;
//                Message message = null;
//
//                try {
//                    for (int i = -1; i < random.nextInt(10); i++) {
//                        // go online and take none
//                        con = createConnection(clientId);
//                        session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//                        consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//                        session.close();
//                        con.close();
//                    }
//
//                    // consume 1
//                    con = createConnection(clientId);
//                    session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//                    consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//                    message = consumer.receive(4000);
//                    assertNotNull("got message", message);
//                    // it is not reliable as it depends on broker dispatch rather than client receipt
//                    // and delivered ack
//                    //  assertFalse("not redelivered", message.getJMSRedelivered());
//                    message.acknowledge();
//                    session.close();
//                    con.close();
//
//                    // peek all
//                    for (int j = -1; j < random.nextInt(10); j++) {
//                        con = createConnection(clientId);
//                        session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//                        consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//
//                        for (int i = 0; i < count - 1; i++) {
//                            assertNotNull("got message", consumer.receive(4000));
//                        }
//                        // no ack
//                        session.close();
//                        con.close();
//                    }
//
//                    // consume remaining
//                    con = createConnection(clientId);
//                    session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//                    consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//
//                    for (int i = 0; i < count - 1; i++) {
//                        message = consumer.receive(4000);
//                        assertNotNull("got message", message);
//                        assertTrue("is redelivered", message.getJMSRedelivered());
//                    }
//                    message.acknowledge();
//                    session.close();
//                    con.close();
//
//                    con = createConnection(clientId);
//                    session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//                    consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//                    assertNull("no message left", consumer.receive(2000));
//                } catch (Throwable throwable) {
//                    throwable.printStackTrace();
//                    exceptions.add(throwable);
//                }
//            }
//        }
//        ExecutorService executorService = Executors.newCachedThreadPool();
//        for (int i=0; i<numClients; i++) {
//            executorService.execute(new Client("cliId" + i));
//        }
//        executorService.shutdown();
//        executorService.awaitTermination(10, TimeUnit.MINUTES);
//        assertTrue("No exceptions expected, but was: " + exceptions, exceptions.isEmpty());
//    }

    public static class Listener implements MessageListener {
        int count = 0;
        String id = null;

        Listener() {
        }
        Listener(String id) {
            this.id = id;
        }
        @Override
        public void onMessage(Message message) {
            count++;
            if (id != null) {
                try {
                    LOG.info(id + ", " + message.getJMSMessageID());
                } catch (Exception ignored) {}
            }
        }
    }

    public class FilterCheckListener extends Listener  {

        @Override
        public void onMessage(Message message) {
            count++;

            try {
                Object b = message.getObjectProperty("$b");
                if (b != null) {
                    boolean c = message.getBooleanProperty("$c");
                    assertTrue("", c);
                } else {
                    String d = message.getStringProperty("$d");
                    assertTrue("", "D1".equals(d) || "D2".equals(d));
                }
            }
            catch (JMSException e) {
                e.printStackTrace();
                exceptions.add(e);
            }
        }
    }
}
