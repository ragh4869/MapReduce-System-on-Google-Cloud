# Packages
import socket
import json
from threading import Thread
import os

# Defining Get function
def get_data(client_data, conn):
    
    # Checking for required number of inputs
    if len(client_data) != 2:
        conn.sendall('Invalid command inputs. Try Again!\r\nEND'.encode())
        return
    
    # Get Key value
    key_val = client_data[1]

    try:
        log_path = ""
        try:
            # Read the intermediary file to get log path
            with open('intermediary_data.txt', 'r') as f:
                lines = f.readlines()
                for line in lines:
                    line_list = line.split(maxsplit=2) 
                    if line_list[0] == "master_node":
                        log_path = line_list[2].replace('\n','')
                        break
                    else:
                        continue
        
            # Update log file
            with open(log_path, 'a') as f: # Open text file to append
                text_val = f"get {key_val}\n"
                f.write(text_val)

            # Send the value of the given key
            comm = ""
            with open('intermediary_data.txt', 'r') as f:  # Open text file to read 
                lines = f.readlines()
                for line in lines:
                    line_list = line.split(maxsplit=2) 
                    if line_list[0] == key_val:
                        comm = f"VALUE {key_val} {line_list[1]} {line_list[2]}\n"
                if comm != "":
                    conn.sendall(comm.encode())
                    return
        except:
            try:
                # Read the intermediary file to get log path with latin-1 encoding
                with open('intermediary_data.txt', 'r', encoding="latin-1") as f:
                    lines = f.readlines()
                    for line in lines:
                        line_list = line.split(maxsplit=2) 
                        if line_list[0] == "master_node":
                            log_path = line_list[2].replace('\n','')
                            break
                        else:
                            continue
                
                # Update log file with latin-1 encoding
                with open(log_path, 'a', encoding="latin-1") as f: # Open text file to read
                    text_val = f"get {key_val}\n"
                    f.write(text_val)

                # Send the value of the given key by reading with latin-1 encoding
                comm = ""
                with open('intermediary_data.txt', 'r', encoding="latin-1") as f:  # Open text file to read 
                    lines = f.readlines()
                    for line in lines:
                        line_list = line.split(maxsplit=2) 
                        if line_list[0] == key_val:
                            comm = f"VALUE {key_val} {line_list[1]} {line_list[2]}\n"
                    if comm != "":
                        conn.sendall(comm.encode())
                        return
            except:
                # Read the intermediary file to get log path with utf-8 encoding
                with open('intermediary_data.txt', 'r', encoding="utf-8") as f:
                    lines = f.readlines()
                    for line in lines:
                        line_list = line.split(maxsplit=2) 
                        if line_list[0] == "master_node":
                            log_path = line_list[2].replace('\n','')
                            break
                        else:
                            continue
                
                # Update log file with utf-8 encoding
                with open(log_path, 'a', encoding="utf-8") as f: # Open text file to read
                    text_val = f"get {key_val}\n"
                    f.write(text_val)

                # Send the value of the given key by reading with utf-8 encoding
                comm = ""
                with open('intermediary_data.txt', 'r', encoding="utf-8") as f:  # Open text file to read 
                    lines = f.readlines()
                    for line in lines:
                        line_list = line.split(maxsplit=2) 
                        if line_list[0] == key_val:
                            comm = f"VALUE {key_val} {line_list[1]} {line_list[2]}\n"
                    if comm != "":
                        conn.sendall(comm.encode())
                        return
                
        conn.sendall('Key value not available\r\nEND'.encode())
    except:
        conn.sendall(f'Text file not available'.encode())


# Defining Set function
def set_data(client_data, conn):
    
    # Checking for required number of inputs
    if len(client_data) != 4:
        conn.sendall('NOT-STORED\r\nInvalid command inputs. Try Again!\r\n'.encode())
        return
    
    # Get Key value
    key_val = client_data[1]
    
    # Checking for correct bit value input
    try:
        bit_value = int(client_data[2])
    except:
        conn.sendall('NOT-STORED\r\nBit value is not an integer. Try Again!\r\n'.encode())
        return

    if bit_value == len(client_data[3]):
        pass
    else:
        bit_value = len(client_data[3])
    
    # Checking if the data is getting STORED or NOT-STORED
    try:
        # Create initial files - intermediary and log file
        if key_val == "master_node":
        
            if not os.path.exists(client_data[3]):
                os.mkdir(client_data[3])    
        
            # Log file creation
            log_path = f"{client_data[3]}//log.txt"
            if not os.path.exists(log_path):
                with open(log_path, 'w') as f: # Write a new text file 
                    text_val = f"set {key_val} {len(log_path)} {log_path}\n"
                    f.write(text_val)
        
            # Intermediary file creation
            if not os.path.exists('intermediary_data.txt'):
                with open('intermediary_data.txt', 'w') as f: # Write a new text file 
                    text_val = f"{key_val} {len(log_path)} {log_path}\n"
                    f.write(text_val)

            conn.sendall('STORED\r\n'.encode())
            return
        
        log_path = ""
        try:
            # Read the intermediary file to get log path
            with open('intermediary_data.txt', 'r') as f:
                lines = f.readlines()
                for line in lines:
                    line_list = line.split(maxsplit=2)
                    if line_list[0] == "master_node":
                        log_path = line_list[2].replace('\n','')
                        break
                    else:
                        continue
        
            # Update log file
            with open(log_path, 'a') as f: # Open text file to read
                text_val = f"set {key_val} {bit_value} {client_data[3]}\n"
                f.write(text_val)
        
            # Set the value of the given key
            with open('intermediary_data.txt', 'a') as f: # Open text file to read
                text_val = f"{key_val} {bit_value} {client_data[3]}\n"
                f.write(text_val)
            
        except:
            try:
                # Read the intermediary file to get log path with latin-1 encoding
                with open('intermediary_data.txt', 'r', encoding="latin-1") as f:
                    lines = f.readlines()
                    for line in lines:
                        line_list = line.split(maxsplit=2)
                        if line_list[0] == "master_node":
                            log_path = line_list[2].replace('\n','')
                            break
                        else:
                            continue
            
                # Update log file with latin-1 encoding
                with open(log_path, 'a', encoding="latin-1") as f: # Open text file to read
                    text_val = f"set {key_val} {bit_value} {client_data[3]}\n"
                    f.write(text_val)
                
                # Set the value of the given key by reading with latin-1 encoding
                with open('intermediary_data.txt', 'a', encoding="latin-1") as f: # Open text file to read
                    text_val = f"{key_val} {bit_value} {client_data[3]}\n"
                    f.write(text_val)
                
            except:
                # Update log file with utf-8 encoding
                with open(log_path, 'a', encoding="utf-8") as f: # Open text file to read
                    text_val = f"set {key_val} {bit_value} {client_data[3]}\n"
                    f.write(text_val)
                
                # Set the value of the given key by reading with utf-8 encoding
                with open('intermediary_data.txt', 'a', encoding="utf-8") as f: # Open text file to read
                    text_val = f"{key_val} {bit_value} {client_data[3]}\n"
                    f.write(text_val)
                
        conn.sendall('STORED\r\n'.encode())
    except:
        conn.sendall(f'NOT-STORED \r\n'.encode())


# Defining delete file function
def del_file(client_data, conn):

    # Checking for required number of inputs
    if len(client_data) != 2:
        conn.sendall('Invalid command inputs. Try Again!\r\nEND'.encode())
        return
    
    # Get file value
    filename = client_data[1]

    try:
        # Check if file exists and then delete it
        if os.path.exists(filename):
            os.remove(filename)
            conn.sendall('File deleted!\r\n'.encode())
        else:
            conn.sendall('File already deleted!\r\n'.encode())
    except:
        conn.sendall('File not deleted! Try Again!'.encode())

# Defining write file function
def write_file(client_data, conn):

    # Checking for required number of inputs
    if len(client_data) != 3:
        conn.sendall('Invalid command inputs. Try Again!\r\nEND'.encode())
        return
    
    # Get file and reducer value
    mapper_func = client_data[1]
    reducers = int(client_data[2])

    # Initialize required variables
    reducer_list = [f"red_{i}_out" for i in range(1, reducers+1)]
    key_value = ""
    kv_store = {}
    task = ""

    try:
        # Get task value and update key_value
        with open('intermediary_data.txt', 'r') as f:
            lines = f.readlines()
            for line in lines:
                line_list = line.split(maxsplit=2)
                if line_list[0] in reducer_list:
                    if len(key_value) == 0:
                        key_value = key_value + line_list[2]
                    else:
                        key_value = key_value + ',' + line_list[2]
                elif line_list[0] == "master_node":
                    task = line_list[2].split('//')[0]
    except:
        try:
            # Get task value and update key_value by reading with utf-8 encoding
            with open('intermediary_data.txt', 'r', encoding="utf-8") as f:
                lines = f.readlines()
                for line in lines:
                    line_list = line.split(maxsplit=2)
                    if line_list[0] in reducer_list:
                        if len(key_value) == 0:
                            key_value = key_value + line_list[2]
                        else:
                            key_value = key_value + ',' + line_list[2]
                    elif line_list[0] == "master_node":
                        task = line_list[2].split('//')[0]
        except:
            # Get task value and update key_value by reading with latin-1 encoding
            with open('intermediary_data.txt', 'r', encoding="latin-1") as f:
                lines = f.readlines()
                for line in lines:
                    line_list = line.split(maxsplit=2)
                    if line_list[0] in reducer_list:
                        if len(key_value) == 0:
                            key_value = key_value + line_list[2]
                        else:
                            key_value = key_value + ',' + line_list[2]
                    elif line_list[0] == "master_node":
                        task = line_list[2].split('//')[0]
    
    # Word Count: Parse the string into dictionary format
    if mapper_func == "map_wc":
        for key_val in key_value.split(','):
            key, val = key_val.split(':')
            if key in kv_store.keys():
                kv_store[key] += int(val)
            else:
                kv_store[key] = int(val)
    # Inveted Index: Parse the string into dictionary format
    else:
        for key_val in key_value.split(','):
            key_doc, val = key_val.split(':')
            doc_id, key = key_doc.split('@')
            if key in kv_store.keys():
                if doc_id in kv_store[key].keys():
                    kv_store[key][doc_id] += int(val)
                else:
                    kv_store[key][doc_id] = int(val)
            else:
                kv_store[key] = {doc_id: int(val)}
    
    filename = task + f"//{task}_output.json"
    
    try:
        # Create json output file 
        if not os.path.exists(filename):
            with open(filename, 'w') as f: # Write a new text file 
                json.dump(kv_store, f, indent = 2)
        
        conn.sendall('Output file created!\r\n'.encode())
    except:
        conn.sendall('Outful file not created! Try Again!'.encode())


# Defining server program
def server_program():

    host = socket.gethostname() # Get hostname
    port = 4869  # Initiate port value greater than 1024

    # Get server instance for IPv4 address and TCP connection
    server_instance = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

    try:
        server_instance.bind((host, port))  # Binding host address and port to the server socket
    except socket.error as se:
        print(str(se)) # Printing socket error

    server_instance.listen() # Instance of server listening to clients 

    # act = True
    while True:
        conn, address = server_instance.accept()  # Accepting a new connection
        print("Connection from: " + str(address))
        msg = []
        while True:
            client_data = conn.recv(5500) # Receive data from client
            # Check for empty data
            if not client_data:
                break
            try:
                response_data = client_data.decode() # Decode the client data
            except:
                try:
                    response_data = client_data.decode('latin-1') # Decode the client data with latin-1
                except:
                    response_data = client_data.decode('utf-8') # Decode the client data with utf-8 
            msg.append(response_data)
            if '\n' in response_data:
                break

        resp = "".join(msg)
        resp = resp.replace('\n','')

        cl_data = resp.split(maxsplit=3) 
        
        # Run the commands using Thread
        if cl_data[0].lower() == 'set':
            # Start Set function thread
            client_thread = Thread(target=set_data, args=(cl_data,conn,))
            client_thread.start()
        elif cl_data[0].lower() == 'get':
            # Start Get function thread
            client_thread = Thread(target=get_data, args=(cl_data,conn,))
            client_thread.start()
        elif cl_data[0].lower() == 'del':
            # Start Delete file function thread
            client_thread = Thread(target=del_file, args=(cl_data,conn,))
            client_thread.start()
        elif cl_data[0].lower() == 'write':
            # Start Write file function thread
            client_thread = Thread(target=write_file, args=(cl_data,conn,))
            client_thread.start()
        else:
            conn.sendall('Wrong Command. Try Again!\r\n'.encode())


    conn.close()

if __name__ == '__main__':
    server_program()
