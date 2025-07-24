import json
from kombu import Connection, Exchange, Queue, Consumer, Producer

# --- B-Tree Implementation (This part remains unchanged) ---
class BTreeNode:
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.keys = []
        self.children = []

class BTree:
    def __init__(self, t):
        self.root = BTreeNode(leaf=True)
        self.t = t

    def search(self, k, x=None):
        if x is None:
            x = self.root
        i = 0
        while i < len(x.keys) and k > x.keys[i]:
            i += 1
        if i < len(x.keys) and k == x.keys[i]:
            return True
        elif x.leaf:
            return False
        else:
            return self.search(k, x.children[i])

    def insert(self, k):
        root = self.root
        if len(root.keys) == (2 * self.t) - 1:
            new_root = BTreeNode()
            self.root = new_root
            new_root.children.insert(0, root)
            self._split_child(new_root, 0)
            self._insert_non_full(new_root, k)
        else:
            self._insert_non_full(root, k)

    def _insert_non_full(self, x, k):
        i = len(x.keys) - 1
        if x.leaf:
            x.keys.append(0)
            while i >= 0 and k < x.keys[i]:
                x.keys[i + 1] = x.keys[i]
                i -= 1
            x.keys[i + 1] = k
        else:
            while i >= 0 and k < x.keys[i]:
                i -= 1
            i += 1
            if len(x.children[i].keys) == (2 * self.t) - 1:
                self._split_child(x, i)
                if k > x.keys[i]:
                    i += 1
            self._insert_non_full(x.children[i], k)

    def _split_child(self, x, i):
        t = self.t
        y = x.children[i]
        z = BTreeNode(leaf=y.leaf)
        x.children.insert(i + 1, z)
        x.keys.insert(i, y.keys[t - 1])
        z.keys = y.keys[t:(2 * t) - 1]
        y.keys = y.keys[0:(t - 1)]
        if not y.leaf:
            z.children = y.children[t:(2 * t)]
            y.children = y.children[0:t]

# --- Kombu RPC Server ---

# Initialize the B-Tree
btree = BTree(3)
sample_data = [10, 20, 5, 6, 12, 30, 7, 17, 3, 1, 4, 15, 18, 25, 28, 32, 35]
for item in sample_data:
    btree.insert(item)
print("B-Tree populated.")

# Define the queue for receiving RPC requests
RPC_QUEUE_NAME = 'rpc_queue'
task_queue = Queue(RPC_QUEUE_NAME, Exchange(''), routing_key=RPC_QUEUE_NAME)

def on_request(body, message):
    """Callback function to handle incoming requests."""
    try:
        key = int(body)
        print(f" [.] Received request to search for key: {key}")
        result = btree.search(key)
        response = {"found": result}
    except (ValueError, TypeError):
        print(f" [!] Invalid request received: {body}")
        response = {"error": "Invalid key provided. Must be an integer."}

    # FIX: Instantiate the Producer class with the message's channel.
    # The message object contains the necessary properties (reply_to, correlation_id)
    # to route the response back to the correct client.
    with Producer(message.channel) as producer:
        producer.publish(
            json.dumps(response),
            exchange='',
            routing_key=message.properties['reply_to'],
            correlation_id=message.properties['correlation_id'],
            content_type='application/json',
            serializer='json' # Ensure the payload is treated as JSON
        )
    
    # Acknowledge the original message to remove it from the queue
    message.ack()

# --- Main execution loop ---
with Connection('amqp://guest:guest@localhost:5672//') as conn:
    with Consumer(conn, queues=task_queue, callbacks=[on_request], accept=['text/plain', 'application/json']):
        print(" [x] Awaiting RPC requests")
        while True:
            try:
                conn.drain_events()
            except (ConnectionResetError, InterruptedError):
                print("Connection lost. Reconnecting...")
                continue