Here is the content of script if you want to hands on to use mage ai for building streaming pipeline.

run this command first: (make sure you have docker installed on your computer).

`docker-compose up -d`

after the initialization of the container is done open the mage UI.

`localhost:6789`

go to the pipeline menu and create a streming pipeline and create this node.

DATA LOADER
```python
from mage_ai.streaming.sources.base_python import BasePythonSource
from typing import Callable
import random
import time

if 'streaming_source' not in globals():
    from mage_ai.data_preparation.decorators import streaming_source


@streaming_source
class CustomSource(BasePythonSource):
    def init_client(self):
        """
        Initialization logic for the source.
        """
        pass

    def batch_read(self, handler: Callable):
        """
        Generate transactions and send them to the handler.
        """
        while True:
            transaction = self.generate_transaction()
            handler([transaction])
            time.sleep(1)  # Simulate a transaction every second

    def generate_transaction(self):
        """
        Generate a single transaction record.
        """
        return {
            'transaction_id': random.randint(1000, 9999),
            'user_id': random.randint(1, 100),
            'amount': random.uniform(10.0, 1000.0),
            'timestamp': time.time(),
            'location': random.choice(['NY', 'CA', 'TX', 'FL', 'IL'])
        }
```

DATA TRANSFORMER

```python
from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(messages: List[Dict], *args, **kwargs):
    """
    Template code for a transformer block.

    Args:
        messages: List of messages in the stream.

    Returns:
        Transformed messages
    """
    def detect_fraud(transaction: Dict) -> bool:
        """
        Simple rule-based fraud detection: flag transactions over $500.
        """
        return transaction['amount'] > 500

    def alert_fraud(transaction: Dict):
        """
        Print an alert for fraudulent transactions.
        """
        print(f"Fraud detected: {transaction}")

    transformed_messages = []
    for message in messages:
        message['is_fraud'] = detect_fraud(message)
        if message['is_fraud']:
            alert_fraud(message)
        transformed_messages.append(message)

    return transformed_messages
```

This are the basic streaming pipeline with alerting but how we can store the data in DB?

As you can see the docker-compose file already set the postgress db from the .env file so we can configure the postgres connection on mage

before we load the data to postgres table we create the table first

Create transaction table
```sql
CREATE TABLE IF NOT EXISTS postgres.magic.transaction (
    transaction_id INT PRIMARY KEY,
    user_id INT,
    amount FLOAT,
    event_timestamp TIMESTAMP,
    location VARCHAR(5),
    is_fraud BOOLEAN
)
```

after creating the table add the last node 

DATA EXPORTER

```python
from mage_ai.streaming.sinks.base_python import BasePythonSink
from typing import List, Dict
import psycopg2

if 'streaming_sink' not in globals():
    from mage_ai.data_preparation.decorators import streaming_sink

@streaming_sink
class CustomSink(BasePythonSink):
    def init_client(self):
        """
        Implement the logic of initializing the client.
        """
        # Database connection details
        self.db_config = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'postgres',
            'host': 'postgres',
            'port': 5432,
        }
        # Establish a database connection
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()

    def batch_write(self, messages: List[Dict]):
        """
        Batch write the messages to the sink.

        For each message, the message format could be one of the following ones:
        1. message is the whole data to be written into the sink
        2. message contains the data and metadata with the format {"data": {...}, "metadata": {...}}
            The data value is the data to be written into the sink. The metadata is used to store
            extra information that can be used in the write method (e.g. timestamp, index, etc.).
        """
        # SQL query to insert data
        insert_query = """
        INSERT INTO magic.transaction (
            transaction_id, user_id, amount, event_timestamp, location, is_fraud
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING;
        """

        # Insert each message into the database
        for msg in messages:
            data = msg.get('data', msg)  # Handles both formats
            self.cursor.execute(
                insert_query,
                (
                    data['transaction_id'],
                    data['user_id'],
                    data['amount'],
                    data['event_timestamp'],
                    data['location'],
                    data['is_fraud'],
                )
            )

        # Commit the transaction
        self.conn.commit()

    def stop(self):
        """
        Close the database connection when the sink is stopped.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def __del__(self):
        # Ensure the database connection is closed when the sink is deleted
        self.stop()

```


