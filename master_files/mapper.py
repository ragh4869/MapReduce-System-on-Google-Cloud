# Packages
import socket
import re
import sys

# Defining mapper class
class map:

    def __init__(self, filename, host):
        self.kv_store = ""
        self.filename = filename
        self.lines = ""
        self.doc_id = self.filename
        self.host = host
        self.port = 4869

    # Defining function to get the word list from line with all transformations
    def line_to_word(self, line):
        line = re.sub(r'[^\x00-\x7F]+',' ', line)
        line = re.sub('\W+',' ', line)
        line = re.sub(r'[0-9]', '', line)
        word_list = line.split()
        # print("Word_list:", word_list)
        return word_list

    # Defining function to set data from KV-store
    def call_set(self, comm):
        
        # Get mapper instance for IPv4 address and TCP connection
        mapper_instance = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            mapper_instance.connect((self.host, self.port))  # Connect to the server
        except socket.error as se:
            print(str(se)) # Printing socket error
        
        mapper_instance.sendall(comm.encode())  # Send command to server
        
        response_data = mapper_instance.recv(1024).decode()  # Receive response from server
        print(response_data)  # Server Response

        mapper_instance.close()  # Close the connection 
    
    # Defining function to get data from KV-store
    def call_get(self, comm):
        
        # Get mapper instance for IPv4 address and TCP connection
        mapper_instance = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            mapper_instance.connect((self.host, self.port))  # Connect to the server
        except socket.error as se:
            print(str(se)) # Printing socket error
        
        mapper_instance.sendall(comm.encode())  # Send command to server
        
        # Looping and combining all the data being sent
        msg = []
        while True:
            client_data = mapper_instance.recv(5500) # Receive data from client
            # Check for empty data
            if not client_data:
                break
            try:
                response_data = client_data.decode() # Decode the client data
            except:
                response_data = client_data.decode('latin-1') # Decode the client data
            # print(response_data)
            msg.append(response_data)
            if '\n' in response_data:
                break

        resp = "".join(msg)
        resp = resp.replace('\n','')
        self.lines = resp.split(maxsplit=3)[3]
        
        mapper_instance.close()  # Close the connection 
        

# Defining mapper function for word-count
def map_wc(filename, host):
    
    # Initialize a mapper class instance
    m = map(filename, host)
    
    # Get the required data to perform the necessary operations
    comm = f"get {m.filename}\n"
    m.call_get(comm)
    
    # Get the word list from all the line
    word_list = m.line_to_word(m.lines) 

    # Convet to required string format as mapper output
    for word in word_list:
        if len(m.kv_store) == 0:
            m.kv_store = m.kv_store + f"{word.lower()}:1"
        else:
            m.kv_store = m.kv_store + f",{word.lower()}:1"

    # Store the mapper output in KV-store
    key = "map_"+m.filename
    comm = f"set {key} {len(m.kv_store)} {m.kv_store}\n"   
    # print(comm) 
    m.call_set(comm)
    
    # Store the status update of the mapper output in KV-store
    key = "map_"+m.filename+"_status"
    comm = f"set {key} 3 yes\n"
    m.call_set(comm)

# Defining mapper function for inverted-index
def map_inv_ind(filename, host):

    # Initialize a mapper class instance
    m = map(filename, host)
    
    # Get the required data to perform the necessary operations
    comm = f"get {m.filename}\n"
    m.call_get(comm)
    
    # Get the word list from all the line
    word_list = m.line_to_word(m.lines) 

    # Convet to required string format as mapper output
    for word in word_list:
        if len(m.kv_store) == 0:
            m.kv_store = m.kv_store + f"{m.doc_id}@{word.lower()}:1"
        else:
            m.kv_store = m.kv_store + f",{m.doc_id}@{word.lower()}:1"

    # Store the mapper output in KV-store
    key = "map_"+m.filename
    comm = f"set {key} {len(m.kv_store)} {m.kv_store}\n"   
    m.call_set(comm)
    
    # Store the status update of the mapper output in KV-store
    key = "map_"+m.filename+"_status"
    comm = f"set {key} 3 yes\n"
    m.call_set(comm)

if __name__ == '__main__':
    if sys.argv[1] == "map_wc":
        map_wc(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == "map_inv_ind":
        map_inv_ind(sys.argv[2], sys.argv[3]) 