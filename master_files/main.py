# Packages
import socket
import json
from multiprocessing import Process
from multiprocessing import Barrier
import os
import sys
from mapper import map_wc, map_inv_ind
from reducer import red_wc, red_inv_ind
from VM import vm_setup, del_vm

# Run the mapper using Process 
def run_mapper(mapper_barrier, mapper_func, file_name, host):

    vm_mapper_name = "map_" + file_name
    vm_mapper_name = vm_mapper_name.replace("_","-").lower()
    vm_setup(vm_mapper_name, "master_files/mapper.py") # Create the new VM instance and copy relevant files

    # Run the mapper function
    os.system(f'gcloud compute ssh {vm_mapper_name} --zone=us-central1-a --command="python3 mapper.py {mapper_func} {file_name} {host}"')  

    # Wait for all the mapper processes to end
    mapper_barrier.wait()

# Run the reducer using Process 
def run_reducer(reducer_barrier, reducer_func, file_name, host):

    reducer_name = file_name.replace("_","-")
    vm_setup(reducer_name, "master_files/reducer.py") # Create the new VM instance and copy relevant files

    # Run the reducer function
    os.system(f'gcloud compute ssh {reducer_name} --zone=us-central1-a --command="python3 reducer.py {reducer_func} {file_name} {host}"')

    # Wait for all the reducer processes to end
    reducer_barrier.wait()
 
# Defining class Master
class Master:

    # Initializing master variables
    def __init__(self, task, filename, mappers, reducers, mapper_func, reducer_func):
        self.task = task
        self.key_value = ""
        self.filename = filename
        self.split_files = []
        self.folder_path = ''
        self.mappers = mappers
        self.reducers = reducers
        self.mapper_func = mapper_func
        self.reducer_func = reducer_func
        self.host = sys.argv[1]
        self.port = 4869
        self.mapper_list = []
        self.reducer_list = [f"red_{i}" for i in range(1, self.reducers+1)]

    # Defining function to check if all mappers have finished running
    def check_mapper_reducer_completion(self, check, check_list):
        c = check
        not_done = []
        while check_list:
            
            check_file = check_list.pop(0)
            status_check_file = check_file+"_status"
            # Get check instance for IPv4 address and TCP connection
            check_instance = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

            try:
                check_instance.connect((self.host, self.port))  # Connect to the server
            except socket.error as se:
                print(str(se)) # Printing socket error

            check_instance.sendall(f"get {status_check_file}\n".encode()) # Send response to server

            response_data = check_instance.recv(1024).decode()  # Receive response from server
            print(response_data) # Server response

            check_instance.close()

            # Get the count of mappers/reducers that have successfully run
            if "yes" in response_data.split()[3]:
                c += 1
            # Get the list of mappers/reducers that have not successfully run
            else:
                not_done.append(check_file)

        # Return the count and list
        return c,not_done
    
    # Defining the function to split the file into chunks
    def file_chunk(self, file_size, sort_list, flag, mapper_count):
        
        # Condition if the file sent is a text file
        if flag == 0:
            l = 1
            with open(self.filename, encoding='utf-8') as f:
                while content := f.read(file_size):
                    key = self.filename.replace(".txt",f"_{l}")
                    self.split_files.append(key) # Append the split file keys
                    content = content.replace('\n',' ')
                    # Store the split file data in KV-store
                    comm = f"set {key} {len(content)} {content}\n"
                    self.call_set_del_write_file(comm)
                    l += 1
        # Condition if the file sent is a folder
        else:
            file_count = len(sort_list) # Get file count
            for i in sort_list:
                l = 1
                f_size = os.path.getsize(self.filename+"//"+i)
                # If number of files and mappers are same, just read the whole file for each mapper
                if file_count == mapper_count:
                    no_of_splits = 1
                # If only one file is left then split it into the number of files required for the remaining mappers
                elif file_count == 1:
                    no_of_splits = mapper_count
                else:
                    no_of_splits = f_size/file_size
                if round(no_of_splits) == 0 or round(no_of_splits) == 1:
                    with open(self.filename+"//"+i, encoding='utf-8') as f:
                        key = i.replace('.txt',f"_{l}")
                        self.split_files.append(key) # Append the split file keys
                        content = f.read()
                        content = content.replace('\n',' ')
                        # Store the split file data in KV-store
                        comm = f"set {key} {len(content)} {content}\n"
                        self.call_set_del_write_file(comm)
                    mapper_count -= 1
                    file_count -= 1
                    continue
                else:
                    f_size = f_size//round(no_of_splits) + 1
                    with open(self.filename+"//"+i, encoding='utf-8') as f:
                        while content := f.read(f_size):
                            key = i.replace('.txt',f"_{l}")
                            self.split_files.append(key) # Append the split file keys
                            content = content.replace('\n',' ')
                            # Store the split file data in KV-store
                            comm = f"set {key} {len(content)} {content}\n"
                            self.call_set_del_write_file(comm)
                            l += 1
                            mapper_count -= 1
                    file_count -= 1

    # Defining function to split the files for m mappers
    def split_file(self):
        d = {}
        file_size = 0
        if ".txt" in self.filename:
            file_size = os.path.getsize(self.filename)
            flag = 0
        else:
            for i in [j for j in os.listdir(self.filename) if '.txt' in j]:
                file_size += os.path.getsize(self.filename+"//"+i)
                d[i] = os.path.getsize(self.filename+"//"+i)
            flag = 1

        file_size = file_size//self.mappers + 1
        d = dict(sorted(d.items(), key = lambda x: x[1], reverse = False))
        sort_list = list(d.keys())    
        
        # Call the file chunk function to split it according to the mappers
        self.file_chunk(file_size, sort_list, flag, self.mappers)

    # Word Count: Parse the mapper outputs into key:value format for reducer input files
    def parse_map_wc(self):
        key_list = ["" for _ in range(self.reducers)] # Creating list of strings for each reducer input 
        for key_val in self.key_value.split(','):
            key, val = key_val.split(':')
            red_val = abs(hash(key)) % self.reducers # Getting hash value
            # Updating the key & value in string format
            if len(key_list[red_val]) == 0:
                key_list[red_val] = key_list[red_val] + f"{key}:{val}"
            else:
                key_list[red_val] = key_list[red_val] + f",{key}:{val}"

        # Return word list
        return key_list

    # Inverted Index: Parse the mapper outputs into doc_id@key:value format for reducer input files
    def parse_map_inv_ind(self):
        key_list = ["" for _ in range(self.reducers)] # Creating list of strings for each reducer input 
        for key_val in self.key_value.split(','):
            key_doc, val = key_val.split(':')
            doc_id, key = key_doc.split('@')
            red_val = abs(hash(key)) % self.reducers # Getting hash value
            # Updating the doc_id, key & value in string format
            if len(key_list[red_val]) == 0:
                key_list[red_val] = key_list[red_val] + f"{doc_id}@{key}:{val}"
            else:
                key_list[red_val] = key_list[red_val] + f",{doc_id}@{key}:{val}"

        # Return word list
        return key_list

    # Defining function to call the set or delete file function
    def call_set_del_write_file(self, comm):
        
        # Get master instance for IPv4 address and TCP connection
        master_instance = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            master_instance.connect((self.host, self.port))  # Connect to the server
        except socket.error as se:
            print(str(se)) # Printing socket error
        
        master_instance.sendall(comm.encode())  # Send command to server
        
        response_data = master_instance.recv(1024).decode()  # Receive response from server
        print(response_data)  # Server Response

        master_instance.close()  # Close the connection

    # Defining function to get data
    def call_get(self, comm):
        
         # Get master instance for IPv4 address and TCP connection
        master_instance = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            master_instance.connect((self.host, self.port))  # Connect to the server
        except socket.error as se:
            print(str(se)) # Printing socket error
        
        master_instance.sendall(comm.encode())  # Send command to server
        
        # Looping and combining all the data being sent
        msg = []
        while True:
            client_data = master_instance.recv(5500) # Receive data from client
            # Check for empty data
            if not client_data:
                break
            response_data = client_data.decode() # Decode the client data
            msg.append(response_data)
            if '\n' in response_data:
                break

        resp = "".join(msg)
        resp = resp.replace('\n','')

        if len(self.key_value) == 0:
            self.key_value = self.key_value + resp.split(maxsplit=3)[3]
        else:
            self.key_value = self.key_value + ',' + resp.split(maxsplit=3)[3]
        
        master_instance.close()  # Close the connection


# Defining server program
def server_program(task, filename, mappers, reducers, mapper_func, reducer_func):

    # Initializing Master class
    M = Master(task, filename, mappers, reducers, mapper_func, reducer_func)

    # Get master instance for IPv4 address and TCP connection
    master_instance = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

    try:
        master_instance.connect((M.host, M.port))  # Connect to the server
    except socket.error as se:
        print(str(se)) # Printing socket error 
    
    master_instance.sendall(f"set master_node {len(M.task)} {M.task}\n".encode())  # Send response to server
    
    response_data = master_instance.recv(1024).decode()  # Receive response from server
    print(response_data) # Server response

    master_instance.close() # Close master connection

    M.split_file() # Split the files by size into m files where m is the number of mappers and update intermediary key-value store
    
    mapper_barrier = Barrier(M.mappers+1) # Barrier implementation for mapper processes

    # Running the m mappers
    for i in M.split_files:
        
        mapper_name = "map_"+i
        M.mapper_list.append(mapper_name) # Appending mapper list

        mapper_process = Process(target=run_mapper, args=(mapper_barrier, M.mapper_func, i, M.host))
        mapper_process.start()

        # M.run_mapper(mapper_barrier, i) # Running mapper process

    # Check if all the mapper processes have completed or not
    print("Main process waiting for all mapper results...")
    mapper_barrier.wait() # Wait till all mapper processes are done

    mapper_list = M.mapper_list.copy() # Getting the list of file status to check for

    # While loop to ensure the check and fault tolerance of mappers till all are successfully completed for the defined number of times (max_tries)
    max_tries, check = 10, 0
    while max_tries:
        # Running the check function
        check,not_done = M.check_mapper_reducer_completion(check, mapper_list)
        print(f"Number of mappers successfully completed: {check}")
        if check == M.mappers:
            break
        else:
            max_tries -= 1
            # Implementing fault tolerance in case any of the mappers have not completed successfully
            new_mapper_barrier = Barrier(len(not_done)+1)
            # Running only the files which were not successfully completed
            for i in not_done:

                file_val = i.replace('map_','')
                vm_mapper_name = i.replace("_","-").lower() # Get Mapper VM name
                del_vm(vm_mapper_name) # Delete the failed Mapper VM

                new_mapper_process = Process(target=run_mapper, args=(new_mapper_barrier, M.mapper_func, file_val, M.host))
                new_mapper_process.start()

            print("Main process waiting for all remaining mapper results...")
            new_mapper_barrier.wait() # Wait till all mapper processes are done
            mapper_list = not_done.copy()

    print("All mapper processes and checks are done!")

    # Deleting all the Mapper VMs
    # for mapper in M.mapper_list:

    #     vm_mapper_name = mapper.replace("_","-").lower() # Get Mapper VM name
    #     del_vm(vm_mapper_name) # Delete the Mapper VM

    # print("All Mapper VMs are deleted!")

    # Implementing hash function and creating reducer files
    for mapper_file in M.mapper_list:
        
        # Getting all the mapper outputs and combining them
        comm = f"get {mapper_file}\n"
        M.call_get(comm)
        
    if M.mapper_func == "map_wc":
        key_list = M.parse_map_wc() # Parse the mapper output to reducer inputs for Word Count
    else:
        key_list = M.parse_map_inv_ind() # Parse the mapper output to reducer inputs for Inverted Index

    for key, val in zip(M.reducer_list, key_list):
        
        # Store the reducer input in KV-store
        comm = f"set {key} {len(val)} {val}\n"
        M.call_set_del_write_file(comm)

    print("Reducer inputs are generated!")

    reducer_barrier = Barrier(M.reducers+1) # Barrier implementation for reducer processes

    # Running the r reducers         
    for file_name in M.reducer_list:

        reducer_process = Process(target=run_reducer, args=(reducer_barrier, M.reducer_func, file_name, M.host))
        reducer_process.start()

    #     # M.run_reducer(reducer_barrier, file_name) # Running reducer process

    # Check if all the reducer processes have completed or not
    print("Main process waiting for all reducer results...")
    reducer_barrier.wait() # Wait till all reducer processes are done

    print("All reducer processes are done!")
    reducer_list = M.reducer_list.copy() # Getting the list of json files to check for

    # While loop to ensure the check and fault tolerance of reducers till all are successfully completed for the defined number of times (max_tries)
    max_tries, check = 10, 0
    while max_tries:
        # Running the check function
        check,not_done = M.check_mapper_reducer_completion(check, reducer_list)
        print(f"Number of reducers successfully completed: {check}")
        if check == M.reducers:
            break
        else:
            max_tries -= 1
            # Implementing fault tolerance in case any of the reducers have not completed successfully
            new_reducer_barrier = Barrier(len(not_done)+1)
            # Running only the files which were not successfully completed
            for i in not_done:

                vm_reducer_name = i.replace("_","-").lower() # Get Mapper VM name
                del_vm(vm_reducer_name) # Delete the failed Mapper VM

                new_reducer_process = Process(target=run_reducer, args=(new_reducer_barrier, M.reducer_func, i, M.host))
                new_reducer_process.start()

            print("Main process waiting for all remaining reducer results...")
            new_reducer_barrier.wait() # Wait till all reducer processes are done
            reducer_list = not_done.copy()

    print("All reducer processes and checks are done!")

    # Write a final json output file using the reducer outputs. This is done with the write file function interacting with the KV-store
    comm = f"write {M.mapper_func} {M.reducers}\n"
    M.call_set_del_write_file(comm)

    # Intermediary file deletions using the delete file function interacting with the KV-store
    comm = f"del intermediary_data.txt\n"
    M.call_set_del_write_file(comm)
 

if __name__ == '__main__':

    # Getting the configuration file to run different tasks
    with open('master_files/config.json') as con:
        config_data = json.load(con)
        
        # Running the task
        for key in config_data.keys():
            filename = config_data[key]["filename"]
            mappers = config_data[key]["mappers"]
            reducers = config_data[key]["reducers"]
            mapper_func = config_data[key]["mapper_func"]
            reducer_func = config_data[key]["reducer_func"]
            server_program(key,filename, mappers, reducers, mapper_func, reducer_func)