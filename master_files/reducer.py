# Packages
import socket
import sys

# Defining reducer class
class red:

    def __init__(self, filename, host):
        self.filename = filename
        self.key_value = ""
        self.host = host
        self.port = 4869
        self.data = {}
        self.kv_store = {}
    
    # Word Count: Parse the reducer input into dictionary format along with counting each word instance
    def parse_red_wc(self):
        for key_val in self.key_value.split(','):
            key, val = key_val.split(':')
            if key in self.kv_store.keys():
                self.kv_store[key] += int(val)
            else:
                self.kv_store[key] = int(val)

    # Inverted Index: Parse the reducer input into dictionary format along with counting each doc_id-word instance
    def parse_red_inv_ind(self):
        for key_val in self.key_value.split(','):
            key_doc, val = key_val.split(':')
            doc_id, key = key_doc.split('@')
            if key in self.kv_store.keys():
                if doc_id[:-2] in self.kv_store[key].keys():
                    self.kv_store[key][doc_id[:-2]] += int(val)
                else:
                    self.kv_store[key][doc_id[:-2]] = int(val)
            else:
                self.kv_store[key] = {doc_id[:-2]: int(val)}

    # Word Count: Parse the dictionary output back into string
    def parse_dict_str_wc(self):
        self.key_value = ""
        for key in self.kv_store.keys():
            val = self.kv_store[key]
            if len(self.key_value) == 0:
                self.key_value = self.key_value + f"{key}:{val}"
            else:
                self.key_value = self.key_value + f",{key}:{val}"

    # Inverted Index: Parse the dictionary output back into string
    def parse_dict_str_inv_ind(self):
        self.key_value = ""
        for key in self.kv_store.keys():
            for doc_id in self.kv_store[key].keys():    
                val = self.kv_store[key][doc_id]
                if len(self.key_value) == 0:
                    self.key_value = self.key_value + f"{doc_id}@{key}:{val}"
                else:
                    self.key_value = self.key_value + f",{doc_id}@{key}:{val}"

    # Defining function to set the data in KV-store
    def call_set(self, comm):
        
        # Get reducer instance for IPv4 address and TCP connection
        reducer_instance = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            reducer_instance.connect((self.host, self.port))  # Connect to the server
        except socket.error as se:
            print(str(se)) # Printing socket error
        
        reducer_instance.sendall(comm.encode())  # Send command to server
        
        response_data = reducer_instance.recv(1024).decode()  # Receive response from server
        print(response_data)  # Server Response

        reducer_instance.close()  # Close the connection 

    # Defining function to get the data from KV-store
    def call_get(self, comm):
        
        # Get mapper instance for IPv4 address and TCP connection
        reducer_instance = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            reducer_instance.connect((self.host, self.port))  # Connect to the server
        except socket.error as se:
            print(str(se)) # Printing socket error
        
        reducer_instance.sendall(comm.encode())  # Send command to server
        
        # Looping and combining all the data being sent
        msg = []
        while True:
            client_data = reducer_instance.recv(5500) # Receive data from client
            # Check for empty data
            if not client_data:
                break
            response_data = client_data.decode() # Decode the client data
            # print(response_data)
            msg.append(response_data)
            if '\n' in response_data:
                break

        resp = "".join(msg)
        resp = resp.replace('\n','')
        self.key_value = resp.split(maxsplit=3)[3]
        
        reducer_instance.close()  # Close the connection 

# Defining reducer function for word-count
def red_wc(filename, host):
    
    # Initialize a reducer class instance
    r = red(filename, host)

    # Get the required data to perform the necessary operations
    comm = f"get {r.filename}\n"
    r.call_get(comm)

    # Parse the reducer input string into a dictionary along with counting of words
    r.parse_red_wc()

    # Parse the dictionary output back into string
    r.parse_dict_str_wc()
    
    # Store the reducer output in KV-store
    key = r.filename + "_out"
    comm = f"set {key} {len(r.key_value)} {r.key_value}\n"
    r.call_set(comm)

    # Store the status update of the reducer output in KV-store
    key = r.filename + "_status"
    comm = f"set {key} 3 yes\n"
    r.call_set(comm)
    
# Defining reducer function for inverted-index
def red_inv_ind(filename, host):
    
    # Initialize an empty key-value store map
    r = red(filename, host)
    
    # Get the required data to perform the necessary operations
    comm = f"get {r.filename}\n"
    r.call_get(comm)
    
    # Parse the reducer input string into a dictionary along with counting of doc_id-word
    r.parse_red_inv_ind()
    
    # Parse the dictionary output back into string
    r.parse_dict_str_inv_ind()
     
    # Store the reducer output in KV-store
    key = r.filename + "_out"
    comm = f"set {key} {len(r.key_value)} {r.key_value}\n"
    r.call_set(comm)
    
    # Store the status update of the reducer output in KV-store
    key = r.filename + "_status"
    comm = f"set {key} 3 yes\n"
    r.call_set(comm)

if __name__ == '__main__':
    if sys.argv[1] == "red_wc":
        red_wc(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == "red_inv_ind":
        red_inv_ind(sys.argv[2], sys.argv[3])