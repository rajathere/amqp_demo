import http.server
import socketserver
import json
from urllib.parse import urlparse, parse_qs
import uuid
from kombu import Connection, Producer, Exchange, Queue, Consumer

# --- Kombu RPC Client Class ---

class BTreeRpcClient:
    """
    A client for making RPC calls via Kombu.
    This class is designed to be instantiated once and reused.
    """

    def __init__(self, connection_string):
        """Initializes the RPC client, its connection, and a dedicated reply queue."""
        self.connection = Connection(connection_string)
        # Each client instance gets its own exclusive queue for replies.
        self.callback_queue = Queue(uuid.uuid4().hex, exclusive=True, auto_delete=True)
        self.response = None
        self.correlation_id = None

    def on_response(self, body, message):
        """
        Callback that is triggered when a response is received.
        FIX: The function signature is corrected to accept both `body` and `message`.
        """
        # Check if the response correlates to the request we sent.
        if self.correlation_id == message.properties.get('correlation_id'):
            self.response = body
            message.ack()

    def call(self, key):
        """
        Sends an RPC request and waits for the response.

        Args:
            key (int): The key to search for in the B-Tree service.

        Returns:
            dict: The JSON response from the service.
        """
        self.response = None
        self.correlation_id = str(uuid.uuid4())

        # The 'with' statement ensures the channel is properly closed.
        with self.connection.channel() as channel:
            producer = Producer(channel)
            
            # Publish the message to the RPC queue, telling the service
            # where to send the reply.
            producer.publish(
                str(key),
                exchange='',
                routing_key='rpc_queue',  # The queue the B-Tree service listens on
                reply_to=self.callback_queue.name,
                correlation_id=self.correlation_id,
                content_type='text/plain'
            )

            # Wait for the response on our dedicated reply queue.
            with Consumer(channel,
                          queues=[self.callback_queue],
                          callbacks=[self.on_response],
                          no_ack=False):
                # Process events until our response is received or we time out.
                while self.response is None:
                    # The drain_events call waits for incoming messages.
                    self.connection.drain_events(timeout=2) # 2-second timeout
        
        return self.response


# --- HTTP Server Setup ---
PORT = 8000

# Create a single, reusable RPC client instance when the server starts.
# This is much more efficient than creating one for every request.
print("Initializing RPC client...")
rpc_client = BTreeRpcClient('amqp://guest:guest@localhost:5672//')
print("RPC client ready.")


class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Handles HTTP requests and uses the global Kombu RPC client."""
    
    def do_GET(self):
        parsed_path = urlparse(self.path)
        if parsed_path.path == '/search':
            query_components = parse_qs(parsed_path.query)
            self.handle_search(query_components)
        else:
            self.send_error(404, "Not Found")

    def handle_search(self, query):
        if 'key' not in query:
            self.send_error(400, "Bad Request: 'key' parameter missing.")
            return
        
        try:
            key_to_search = int(query['key'][0])
        except ValueError:
            self.send_error(400, "Bad Request: 'key' must be an integer.")
            return

        print(f"Received HTTP request for key: {key_to_search}. Making RPC call via Kombu...")
        
        try:
            # Use the global RPC client instance to make the call
            response = rpc_client.call(key_to_search)
            
            if response is None:
                raise TimeoutError("RPC call timed out. The B-Tree service may be down or busy.")

            print(f"Received RPC response: {response}")

            # Prepare and send the final HTTP response
            response_data = {
                "key_searched": key_to_search,
                "found": response.get('found'),
                "error": response.get('error')
            }
            
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(bytes(json.dumps(response_data), "utf8"))

        except Exception as e:
            print(f"An error occurred during RPC call: {e}")
            self.send_error(503, "Service Unavailable: The data service could not be reached.")


# --- Start the HTTP Server ---
if __name__ == "__main__":
    try:
        import kombu
    except ImportError:
        print("Error: The 'kombu' library is not installed.")
        print("Please install it using: pip install kombu")
        exit(1)

    with socketserver.TCPServer(("", PORT), MyHttpRequestHandler) as httpd:
        print(f"Application server listening on port {PORT}")
        httpd.serve_forever()
