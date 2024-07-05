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


