import requests
import json

# The base URL of the application server
SERVER_BASE_URL = "http://localhost:8000"

def search_for_key(key):
    """
    Sends a GET request to the application server to search for a key.
    """
    # Construct the full URL with the query parameter
    search_url = f"{SERVER_BASE_URL}/search?key={key}"
    print(f"Sending request to: {search_url}")
    
    try:
        # Send the GET request
        response = requests.get(search_url, timeout=10) # Added a timeout

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response from the server
            data = response.json()
            
            print("\n--- Server Response ---")
            if data.get("found"):
                print(f"SUCCESS: Key '{data.get('key_searched')}' was found.")
            else:
                print(f"FAILURE: Key '{data.get('key_searched')}' was NOT found.")
            if data.get("error"):
                print(f"Server error message: {data.get('error')}")
            print("-----------------------")
            
        else:
            # If the status code is not 200, print an error
            print(f"\nError: Received status code {response.status_code}")
            print(f"Response from server: {response.text}")

    except requests.exceptions.ConnectionError:
        # Handle cases where the server is not running or reachable
        print("\nError: Could not connect to the application server.")
        print("Please make sure the application server (server.py) is running.")
    except requests.exceptions.Timeout:
        print("\nError: The request timed out.")
        print("The server might be busy or the backend data service is down.")
    except Exception as e:
        # Handle other potential errors
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    # Ensure the requests library is installed
    try:
        import requests
    except ImportError:
        print("Error: The 'requests' library is not installed.")
        print("Please install it using: pip install requests")
        exit(1)

    print("Client to search for a key in the server's B-Tree.")
    print("Type 'exit' to quit.")
    
    while True:
        # Prompt the user for input
        user_input = input("\nEnter a number to search for: ")
        
        if user_input.lower() == 'exit':
            break
            
        # Validate that the input is a number
        try:
            key_to_find = int(user_input)
            search_for_key(key_to_find)
        except ValueError:
            print("Invalid input. Please enter an integer.")
