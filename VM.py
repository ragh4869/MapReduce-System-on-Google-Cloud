# Packages
import os
import sys
import json
from threading import Thread
from multiprocessing import Process
# from google.cloud import storage
# from google.cloud import datastore
import time 

# Defining function to set up the firewall rules for the project
def firewall_setup(project_id):
    # Setting firewall rules
    os.system(f"gcloud compute --project={project_id} networks create default")
    os.system(f"gcloud compute --project={project_id} firewall-rules create default-allow-icmp --network default --allow icmp --source-ranges 0.0.0.0/0")
    os.system(f"gcloud compute --project={project_id} firewall-rules create default-allow-ssh --network default --allow tcp:22 --source-ranges 0.0.0.0/0")
    os.system(f"gcloud compute --project={project_id} firewall-rules create default-allow-internal --network default --allow tcp:0-65535,udp:0-65535,icmp --source-ranges 10.128.0.0/9")
    os.system(f"gcloud compute --project={project_id} firewall-rules create default-ssh --direction=INGRESS --priority=1000 --network=default --action=ALLOW --rules=tcp:22,tcp:3389 --source-ranges=0.0.0.0/0")
    os.system(f"gcloud compute --project={project_id} firewall-rules create default-internal --direction=INGRESS --priority=1000 --network=default --action=ALLOW --rules=all")
    os.system(f"gcloud compute --project={project_id} firewall-rules create default-allow-http --direction=INGRESS --priority=1000 --network=default --action=ALLOW --rules=tcp:80 --source-ranges=0.0.0.0/0 --target-tags=http-server")

    # List of firewall rules
    # os.system("gcloud compute firewall-rules list")


# Defining function to set up the VM
def vm_setup(vm_name, file_path):

    # Setting up VM and copying the relevant files/folder into the VM
    os.system(f'gcloud compute instances create {vm_name} --zone=us-central1-a --scopes=cloud-platform') # Create the VM
    os.system(f'gcloud compute ssh --zone=us-central1-a {vm_name}') # Get the ssh key for the VM
    os.system(f'gcloud compute scp --recurse --zone=us-central1-a {file_path} {vm_name}:') # Copy relevant files/folder from local to VM

    # Install relevant packages to run the script
    os.system(f'gcloud compute ssh {vm_name} --zone=us-central1-a --command="sudo apt install python3-pip --assume-yes"')
    os.system(f'gcloud compute ssh {vm_name} --zone=us-central1-a --command="sudo apt install -y nginx"')
    os.system(f'gcloud compute ssh {vm_name} --zone=us-central1-a --command="pip3 install numpy"')
    # os.system(f'gcloud compute ssh {vm_name} --zone=us-central1-a --command="pip3 install google"')
    # os.system(f'gcloud compute ssh {vm_name} --zone=us-central1-a --command="pip3 install google-cloud-storage"')
    # os.system(f'gcloud compute ssh {vm_name} --zone=us-central1-a --command="pip3 install google-cloud-datastore"')
    

# Defining the function to delete the Server, Client VMs along with the google cloud storage buckets
def del_vm(vm_name):

    os.system(f"gcloud compute instances delete {vm_name} --zone=us-central1-a --delete-disks=all") # Delete the VM


# Defining function to automate all process and run the VM
def run_vm():

    # Set the project
    project_id = sys.argv[1]
    os.system(f"gcloud config set project {project_id}")

    # Setting Google Credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "raghav-cskumar-fall2022-387fa080baee.json"
    
    # firewall_setup(project_id) # Set up firewall rules for the project

    vm_setup('server-vm', 'server_files') # Setup the Server VM
    vm_setup('master-vm', 'master_files') # Setup the Master VM

    os.system(f'gcloud compute ssh --zone=us-central1-a master-vm') # Get the ssh key for the Client VM

    # Getting server natIP
    os.system('gcloud compute instances list --format=json > ipaddress.json') # Creating json file for running VMs
    with open('ipaddress.json') as f:
        instances = json.load(f)
        for vm_instance in instances:
            if vm_instance['name'] == 'server-vm':
                server_address = vm_instance['networkInterfaces'][0]['networkIP']
    #             # server_address = vm_instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']

    print(f"Server Address: {server_address}")

    # The server-vm has to be started by opening the command prompt and running the code: gcloud compute ssh --zone=us-central1-a server-vm
    # After this, you should navigate to server_files: cd server_files/ 
    # Run the command python3 server.py 

    # Running Storage - native
    while True:
        native_server = input("Is the native server code running in server-vm (y/n): ")
        if native_server.lower() == "y":
            break
        elif native_server.lower() == "n":
            print("Run the native server code in the server-vm!")
            continue
        else:
            print("Wrong Input!")
            continue

    print("Running the native for master!")
    os.system(f'gcloud compute ssh master-vm --zone=us-central1-a --command="python3 master_files/main.py {server_address}"') # Run the main.py file in Master VM

    # Copy the output files to local
    os.system("gcloud compute scp --recurse --zone=us-central1-a server-vm:server_files/inverted_index output")

    # Delete all the VMs
    os.system('gcloud compute instances list --format=json > ipaddress.json') # Creating json file for running VMs
    with open('ipaddress.json') as f:
        instances = json.load(f)
        for vm_instance in instances:
            del_vm(vm_instance['name'])


if __name__ == '__main__':
    run_vm()