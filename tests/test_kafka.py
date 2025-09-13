import logging
import json
import time
from typing import Dict, Any, Optional, List

from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class KafkaAdminClient:
    def __init__(self):
        conf = {
            'bootstrap.servers': 'localhost:9092',
        }
        self.admin_client = AdminClient(conf)

    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        """
        Create a Kafka topic

        Args:
            topic_name (str): Name of the topic to create
            num_partitions (int): Number of partitions (default: 1)
            replication_factor (int): Replication factor (default: 1)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )

            futures = self.admin_client.create_topics([topic])

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic '{topic}' created successfully")
                    return True
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.warning(f"Topic '{topic}' already exists")
                        return True
                    else:
                        logger.error(f"Failed to create topic '{topic}': {e}")
                        return False
        except Exception as e:
            logger.error(f"Error creating topic: {e}")
            return False

    def delete_topic(self, topic_name):
        """
        Delete a Kafka topic

        Args:
            topic_name (str): Name of the topic to delete

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            futures = self.admin_client.delete_topics([topic_name])

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic '{topic}' deleted successfully")
                    return True
                except Exception as e:
                    if "does not exist" in str(e).lower():
                        logger.warning(f"Topic '{topic}' does not exist")
                        return True
                    else:
                        logger.error(f"Failed to delete topic '{topic}': {e}")
                        return False
        except Exception as e:
            logger.error(f"Error deleting topic: {e}")
            return False

    def list_topics(self):
        """
        List all topics

        Returns:
            list: List of topic names
        """
        try:
            metadata = self.admin_client.list_topics()
            topics = list(metadata.topics.keys())
            logger.info(f"Found {len(topics)} topics")
            return topics
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            return []


class KafkaProducer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        """
        Initialize Kafka Producer

        Args:
            bootstrap_servers (str): Kafka bootstrap servers
        """
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'kafka-producer'
        }
        self.producer = Producer(self.conf)

    def produce_message(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Produce a message to Kafka topic

        Args:
            topic (str): Topic name
            message (Dict[str, Any]): Message to send
            key (Optional[str]): Message key

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            serialized_message = json.dumps(message).encode('utf-8')

            def delivery_callback(err, msg):
                if err:
                    logger.error(f"Message delivery failed: {err}")
                else:
                    logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

            self.producer.produce(
                topic=topic,
                value=serialized_message,
                key=key.encode('utf-8') if key else None,
                callback=delivery_callback
            )

            logger.info("flushing")
            self.producer.flush()
            return True

        except Exception as e:
            logger.error(f"Error producing message: {e}")
            return False

    def produce_batch(self, topic: str, messages: List[Dict[str, Any]], keys: Optional[List[str]] = None) -> int:
        """
        Produce multiple messages to Kafka topic

        Args:
            topic (str): Topic name
            messages (List[Dict[str, Any]]): List of messages to send
            keys (Optional[List[str]]): List of message keys

        Returns:
            int: Number of successfully produced messages
        """
        success_count = 0

        for i, message in enumerate(messages):
            key = keys[i] if keys and i < len(keys) else None
            if self.produce_message(topic, message, key):
                success_count += 1

        return success_count

    def close(self):
        """Close the producer"""
        self.producer.flush()


class KafkaConsumer:
    def __init__(self, topics: List[str], group_id: str = 'test-group', bootstrap_servers: str = 'localhost:9092'):
        """
        Initialize Kafka Consumer

        Args:
            topics (List[str]): List of topics to subscribe to
            group_id (str): Consumer group ID
            bootstrap_servers (str): Kafka bootstrap servers
        """
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe(topics)

    def consume_messages(self, timeout: float = 5.0, max_messages: int = 10) -> List[Dict[str, Any]]:
        """
        Consume messages from subscribed topics

        Args:
            timeout (float): Timeout in seconds
            max_messages (int): Maximum number of messages to consume

        Returns:
            List[Dict[str, Any]]: List of consumed messages
        """
        messages = []
        start_time = time.time()

        try:
            while len(messages) < max_messages and (time.time() - start_time) < timeout:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"Reached end of partition for topic {msg.topic()}")
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break

                try:
                    message_data = {
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'key': msg.key().decode('utf-8') if msg.key() else None,
                        'value': json.loads(msg.value().decode('utf-8'))
                    }
                    messages.append(message_data)
                    logger.info(f"Consumed message from {msg.topic()}: {message_data['value']}")

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except Exception as e:
            logger.error(f"Error consuming messages: {e}")

        return messages

    def close(self):
        """Close the consumer"""
        self.consumer.close()


class TestKafkaAdminClient:
    def test_basic(self):
        manager = KafkaAdminClient()
        manager.create_topic("test-topic", num_partitions=1, replication_factor=1)
        topics = manager.list_topics()
        print(f"Current topics: {topics}")
        manager.delete_topic("test-topic")
        print(f"Current topics: {topics}")

    def test_create_topic(self):
        admin = KafkaAdminClient()

        result = admin.create_topic("test-create-topic", num_partitions=1, replication_factor=1)
        assert result == True, "Topic creation should succeed"

        topics = admin.list_topics()
        assert "test-create-topic" in topics, "Created topic should be in topics list"

        admin.delete_topic("test-create-topic")

    def test_delete_topic(self):
        admin = KafkaAdminClient()

        admin.create_topic("test-delete-topic")
        result = admin.delete_topic("test-delete-topic")
        assert result == True, "Topic deletion should succeed"

        topics = admin.list_topics()
        assert "test-delete-topic" not in topics, "Deleted topic should not be in topics list"

    def test_list_topics(self):
        admin = KafkaAdminClient()

        topics = admin.list_topics()
        assert isinstance(topics, list), "list_topics should return a list"


class TestKafkaProducer:
    def setup_method(self):
        self.admin = KafkaAdminClient()
        self.test_topic = "test-producer-topic"
        self.admin.create_topic(self.test_topic)
        self.producer = KafkaProducer()

    def teardown_method(self):
        self.producer.close()
        self.admin.delete_topic(self.test_topic)

    def test_produce_message(self):
        message = {"id": 1, "data": "test message"}
        result = self.producer.produce_message(self.test_topic, message)
        assert result == True, "Message production should succeed"

    def test_produce_message_with_key(self):
        message = {"id": 2, "data": "test message with key"}
        key = "test-key"
        result = self.producer.produce_message(self.test_topic, message, key)
        assert result == True, "Message production with key should succeed"

    def test_produce_batch(self):
        messages = [
            {"id": 1, "data": "batch message 1"},
            {"id": 2, "data": "batch message 2"},
            {"id": 3, "data": "batch message 3"}
        ]
        keys = ["key1", "key2", "key3"]

        success_count = self.producer.produce_batch(self.test_topic, messages, keys)
        assert success_count == 3, "All batch messages should be produced successfully"

    def test_produce_batch_without_keys(self):
        messages = [
            {"id": 4, "data": "batch message without key 1"},
            {"id": 5, "data": "batch message without key 2"}
        ]

        success_count = self.producer.produce_batch(self.test_topic, messages)
        assert success_count == 2, "All batch messages without keys should be produced successfully"


class TestKafkaConsumer:
    def setup_method(self):
        self.admin = KafkaAdminClient()
        self.test_topic = "test-consumer-topic"
        self.admin.create_topic(self.test_topic)
        self.producer = KafkaProducer()
        self.consumer = KafkaConsumer([self.test_topic], group_id="test-consumer-group")

    def teardown_method(self):
        self.consumer.close()
        self.producer.close()
        self.admin.delete_topic(self.test_topic)

    def test_consume_messages(self):
        test_messages = [
            {"id": 1, "data": "consumer test message 1"},
            {"id": 2, "data": "consumer test message 2"}
        ]

        for message in test_messages:
            self.producer.produce_message(self.test_topic, message)

        time.sleep(30)

        consumed_messages = self.consumer.consume_messages(timeout=10.0, max_messages=5)

        assert len(consumed_messages) >= 2, "Should consume at least 2 messages"

        consumed_values = [msg['value'] for msg in consumed_messages]
        for test_msg in test_messages:
            assert test_msg in consumed_values, f"Message {test_msg} should be consumed"

    def test_consume_messages_with_timeout(self):
        consumed_messages = self.consumer.consume_messages(timeout=2.0, max_messages=5)
        assert isinstance(consumed_messages, list), "Should return a list even with timeout"

    def test_consume_messages_with_key(self):
        test_message = {"id": 3, "data": "message with key"}
        test_key = "test-consumer-key"

        self.producer.produce_message(self.test_topic, test_message, test_key)
        time.sleep(2)

        consumed_messages = self.consumer.consume_messages(timeout=10.0, max_messages=1)

        assert len(consumed_messages) >= 1, "Should consume at least 1 message"

        message_with_key = None
        for msg in consumed_messages:
            if msg['value'] == test_message:
                message_with_key = msg
                break

        assert message_with_key is not None, "Should find the message with key"
        assert message_with_key['key'] == test_key, "Message key should match"


class TestKafkaIntegration:
    def setup_method(self):
        self.admin = KafkaAdminClient()
        self.test_topic = "test-integration-topic"
        self.admin.create_topic(self.test_topic, num_partitions=1, replication_factor=1)

    def teardown_method(self):
        self.admin.delete_topic(self.test_topic)

    def test_producer_consumer_workflow(self):
        producer = KafkaProducer()
        consumer = KafkaConsumer([self.test_topic], group_id="integration-test-group")

        try:
            test_messages = [
                {"id": 1, "action": "create", "data": "integration test 1"},
                {"id": 2, "action": "update", "data": "integration test 2"},
                {"id": 3, "action": "delete", "data": "integration test 3"}
            ]

            message_keys = ["key1", "key2", "key3"]

            success_count = producer.produce_batch(self.test_topic, test_messages, message_keys)
            assert success_count == 3, "All messages should be produced successfully"

            time.sleep(3)

            consumed_messages = consumer.consume_messages(timeout=15.0, max_messages=10)

            assert len(consumed_messages) >= 3, "Should consume at least 3 messages"

            consumed_values = [msg['value'] for msg in consumed_messages]
            for test_msg in test_messages:
                assert test_msg in consumed_values, f"Message {test_msg} should be consumed"

            consumed_keys = [msg['key'] for msg in consumed_messages if msg['key'] is not None]
            for key in message_keys:
                assert key in consumed_keys, f"Key {key} should be present in consumed messages"

        finally:
            consumer.close()
            producer.close()

    def test_multiple_consumers_same_group(self):
        producer = KafkaProducer()
        consumer1 = KafkaConsumer([self.test_topic], group_id="same-group")
        consumer2 = KafkaConsumer([self.test_topic], group_id="same-group")

        try:
            test_messages = [
                {"id": i, "data": f"multi-consumer message {i}"}
                for i in range(1, 11)
            ]

            producer.produce_batch(self.test_topic, test_messages)
            time.sleep(2)

            consumed1 = consumer1.consume_messages(timeout=5.0, max_messages=10)
            consumed2 = consumer2.consume_messages(timeout=5.0, max_messages=10)

            total_consumed = len(consumed1) + len(consumed2)
            assert total_consumed >= len(test_messages), "All messages should be consumed across both consumers"

            all_consumed_values = [msg['value'] for msg in consumed1 + consumed2]
            for test_msg in test_messages:
                assert test_msg in all_consumed_values, f"Message {test_msg} should be consumed by one of the consumers"

        finally:
            consumer1.close()
            consumer2.close()
            producer.close()

    def test_topic_management_integration(self):
        test_topic_name = "test-management-topic"

        success = self.admin.create_topic(test_topic_name, num_partitions=1, replication_factor=1)
        assert success == True, "Topic creation should succeed"

        topics = self.admin.list_topics()
        assert test_topic_name in topics, "Created topic should be listed"

        producer = KafkaProducer()
        test_message = {"id": 1, "data": "management test"}

        result = producer.produce_message(test_topic_name, test_message)
        assert result == True, "Should be able to produce to newly created topic"

        consumer = KafkaConsumer([test_topic_name], group_id="management-test-group")
        time.sleep(2)

        consumed_messages = consumer.consume_messages(timeout=5.0, max_messages=1)
        assert len(consumed_messages) >= 1, "Should be able to consume from newly created topic"

        consumer.close()
        producer.close()

        success = self.admin.delete_topic(test_topic_name)
        assert success == True, "Topic deletion should succeed"

        topics = self.admin.list_topics()
        assert test_topic_name not in topics, "Deleted topic should not be listed"
