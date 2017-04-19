#!/usr/bin/python

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from time import sleep
from os import getpid
from threading import Thread
from requests import get
from sys import exit


# Global Variable definition
workerhost = []
balancerhost = []
verbose = True
id = 1


# Interface to store every worker load and up/down information,
# Can volatile for debugging; MUST be implemented into stable storage later
class workerLoad():

    # Called after configuration file has been loaded,
    # Initiate the data needed to store every worker load information
    def init_data(self):
        global workerhost
        # TODO: Implement this method (can be volatile for now or stable if possible)
        pass

    # Set the load of the appropriate worker,
    # Load of -1 means the node is down for now
    def set_load(self, id, load):
        # TODO: Implement this method
        pass

    # Return the id of the most idle worker host
    def get_idle_worker(self):
        # TODO: Implement this method
        pass


# Interface to store the RAFT log, where each "act" is an invocation of the workerLoad set_load method
# Can volatile for debugging; MUST be implemented into stable storage later
class loadLog():

    # Called after configuration file has been loaded,
    # Initiate the data needed to store the RAFT log
    def init_data(self):
        # TODO: Implement this method (can be volatile for now or stable if possible)
        pass

    # Return wether or not a log with the supplied id can be filled with the supplied term
    def is_consistent(self, log_id, term):
        # TODO: Implement this method
        pass

    # Append the log with the supplied data if consistent
    def append_log(self, term, worker_id, worker_load):
        # TODO: Implement this methods
        pass

    # Replace the log with the supplied data if consistent
    # If the id is in the middle, then delete all the following log
    def replace_log(self, log_id, term, worker_id, worker_load):
        # TODO: Implement this methods
        pass

    # Mark the supplied id log as committed, and pass the changes instructed to the workerLoad object
    def commit_log(self, id, worker_load):
        # TODO: Implement this methods
        pass


# Method to load the conf.txt file, and write to workerhost and balancerhost
def load_conf():
    # Try opening the config file
    try:
        conf = open("conf.txt","r",1)
    except IOError:
        print("Error, conf.txt file not found")
        exit()

    # Set the worker and loadbalancer host list
    workercount = int(conf.__next__())
    for index in range(workercount):
        workerhost.append(conf.__next__().rstrip())
    balancercount = int(conf.__next__())
    for index in range(balancercount):
        balancerhost.append(conf.__next__().rstrip())

    # Close the config file
    conf.close()
    print("Config file loaded successfully")


# Get the desired port and the server ID from the conf
def get_port():
    global id
    print("There seems to be " +balancerhost.__len__().__str__()+ " balancer hosts in the system:")
    for host in balancerhost:
        print(id.__str__() +". "+ host.__str__())
        id += 1

    try:
        id = int(input("Which one am I supposed to be? "))
        start = balancerhost[id-1].find(":", 8)
        end = balancerhost[id-1].find("/", start)
        return int(balancerhost[id-1][start+1:end])
    except:
        print("Error in parsing port number")
        exit()


def main():
    load_conf()

    # Get the desired port and prepare the server
    current_port = get_port()
    print(current_port)


if __name__ == "__main__": main()
