<h1 align="center">
MapReduce System on Google Cloud
</h1>

### Objective:

The main objective of this project is to use GCP Virtual machines to deploy the distributed MapReduce System on Google Cloud.

### Functionalities: 
*	**Nodes/Tasks:** Nodes and tasks are implemented as conventional OS processes. Each task is communicated and executed through network sockets. 
*	**Data Storage:** The native data storage (files stored in server-vm) is being used for the key-value store (KV-store) where the input data, intermediate data and output data are accessed through the master node, mapper and reducer for any file manipulations. The files being generated and stored  are being replicated as the distributed file system such as HDFS. In addition to the set_data and get_data functions, the del_file and write_file functions are defined to handle the deletion of files and writing of files. 
*	**Master Node:** The master node is spawned as the master-vm which spawns multiple VMs for the map and reduce tasks. Additionally, it controls and coordinates all the other processes. 
*	**User Interaction:** Inputs are taken in and run through the usage of configuration file stored in the form of json file, thus, the user does not need to input anything from their end. If any changes are required, it can be changed in the configuration file. 
*	**Fault Tolerance:** The code also implements fault tolerance to survive process failures. This is done by restarting only the failed processes and VMs. A max_tries variable is defined to set the number of times it tries to rectify the failures. 

### Applications:

#### **Word-count:** 

Find the frequency of the words in a given text file

**Output:** word1: frequency1, word2: frequency2

#### **Inverted-index:** 

Find the frequency of the words with respect to each of the given files.

**Output:** word1: {doc1: frequency_word1, doc2: frequency_word1, doc3: frequency_word1}

### Implementation: 

This has been explained in detail in the project MapReduced-From-Scratch and the same has been used for this project with the usage of Virtual Machines (VMs). The link for the same is here: https://lnkd.in/gY2nXewm

### How to run or perform the tests using the code?

The **VM.py** file is a singular code file which contains all the operations that need to be done from creating the VMs, split files, run the map-reduce system and deleting all the created VMs. 

Moreover, increase the IN_USE_ADDRESSES of us-central1 to more than 20 from the default 8 as the number of VM instances to be called will be around 15-20. 

Apart from running this single file, you will be prompted 1 time to run the Server VM through the command prompt as shown below:
* The server-vm has to be started by opening the command prompt and running the code: gcloud compute ssh --zone=us-central1-a server-vm
* After this, you should navigate to server_files: cd server_files/ 
* Run the command python3 server.py 

#### Code changes and file addition:

Since I have used the native storage on cloud (storing data on Server VM) as it is the fastest, if you choose to use any GCP storage like GCP Bucket, do the following changes -
* Add the service account key file to the server_files
* The code changes that need to be done is for the change in the name of the service account key (json file). This name change needs to be implemented in 2 code files which are **VM.py** and **server.py**. In the below code - 

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "raghav-cskumar-fall2022-387fa080baee.json"

Instead of "raghav-cskumar-fall2022-387fa080baee.json", replace it with your service account key name (json file name).

* VM.py: Line 57 (in both VM.py files)
* server.py : This code is not added and this can be added under the server_program()

### Process Flow:

The following is the process flow for the Map-Reduce system –
1.	Set the relevant project with user input.
2.	Set the firewall rules if necessary.
3.	Create the server-vm and master-vm with necessary package installations and file transfers.
4.	Get the IP address of the server-vm to be used to connect by the spawned VMs.
5.	The server.py code is run by the user and the master-vm is started with running main.py.
6.	An intermediary file along with a log file is created.
7.	The files are split according to the number of defined mappers.
8.	The split file chunks are sent to each mapper which are run in spawned VMs by process.
9.	A fault tolerance check is performed where the failed Mapper VMs are rerun.
10.	The hashmap groupby is implemented to gather the mapper output data into generating the required number of reducer input files.
11.	The reducer inputs are sent to each reducer which are run in spawned VMs by process.
12.	A fault tolerance check is performed where the failed Reducer VMs are rerun.
13.	The reducer outputs are gathered into creating a final json output file.
14.	The intermediary file is deleted.
15.	The output files in server-vm are copied to local.
16.	All the created VMs are deleted.

### Outputs: Final json output and log file

![Final Output Json](https://user-images.githubusercontent.com/96961381/210895852-7b41502f-f0d3-4ca8-9bfa-9659b7f87077.jpeg)

![Log File](https://user-images.githubusercontent.com/96961381/210895854-5d20a0b9-889b-4059-a62a-f582dc894769.jpeg)
