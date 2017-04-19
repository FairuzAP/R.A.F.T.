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
append_delay = 0.5
verbose = True
id = 1


class workerLoad():
    # Interface to store every worker load and up/down information,
    # Can volatile for debugging; MUST be implemented into stable storage later

    def init_data(self):
        # Called after configuration file has been loaded,
        # Initiate the data needed to store every worker load information
        global workerhost
        # TODO: Implement this method (can be volatile for now or stable if possible)
        pass

    def set_load(self, id, load):
        # Set the load of the appropriate worker, Load of -1 means the node is down for now
        pass

    def get_idle_worker(self):
        # Return the id of the most idle worker host
        pass


class loadLog():
    # Interface to store the RAFT log, where each "act" is an invocation of the workerLoad set_load method
    # Can volatile for debugging; MUST be implemented into stable storage later

    def init_data(self):
        # Called after configuration file has been loaded,
        # Initiate the data needed to store the RAFT log
        # TODO: Implement this method (can be volatile for now or stable if possible)
        pass

    def is_consistent(self, log_id, term):
        # Return wether or not a log with the supplied id can be filled with the supplied term
        pass

    def append_log(self, term, worker_id, worker_load):
        # Append the log with the supplied data and return true if consistent, else return false
        pass

    def replace_log(self, log_id, term, worker_id, worker_load):
        # Replace the log with the supplied data and return true if consistent, else return false
        # If the id is in the middle, then delete all the following log
        pass

    def get_log(self, log_id):
        # Return the log object in this id in form of a dictionary; None if doesn't exist
        return {
            'log_id' : 0,
            'log_term' : 0,
            'worker_id' : 0,
            'worker_load' : 0
        }

    def commit_log(self, log_id, worker_load):
        # Mark the supplied id log as committed, and pass the changes instructed to the workerLoad object
        # If the supplied id is already committed, then return true
        # If id-1 hasn't been commited yet, then something BAD is going on
        pass

    def is_commited(self, log_id):
        # Return wether or not the log with the supplied id is commited
        pass

    def get_size(self):
        # Return the next log id (the one not used yet)
        pass

    def get_last_commited_id(self):
        pass


# Global variables containing server critical information
worker_load = workerLoad()  # The global worker load storage interface
load_log = loadLog()        # The global RAFT log storage interface
state = 0                   # The state of the server, 0=Follower, 1=Candidate, 2=Leader
# TODO: Move the variables below into being stored in a stable storage
curr_term = 0               # The server's current Term
voted_for = None            # The server id that this server voted for this term


class Heartbeat(Thread):
    # The heartbeat daemon thread subclass; Several will be summoned by a new leader, and killed if the owner stepped down
    # If the target's log is consistent with the leader's send empty heartbeat periodically Else, begin converting the log
    # into the leader's one by one (especially when a new record is appended, every other node will be inconsistent)
    # This way, the contains of load_log will automatically be broadcasted at any point

    def __init__(self, target_id):
        self.target_url = balancerhost[target_id]
        Thread.__init__(self)

    def do_append_entry(self, **kwargs):
        # Do an append entry RPC to self.target_url
        # kwargs should contains leader_term, leader_id, prev_log_idx, prev_log_term, worker_id, worker_load, commit_idx
        # Return the results of 'success' and 'term' in form of dictionary
        # TODO: Implement this method
        return {
            'success' : False,
            'term' : 0
        }

    def run(self):
        # An infinite loop of spamming the target with append entry RPC if not consistent with leader
        # Or periodically send heartbeat if the target is consistent
        # The moment curr_term is changed, will terminate automatically

        next_index = load_log.get_size()
        this_term = curr_term

        while this_term == curr_term:
            prev_log = load_log.get_log(next_index-1)
            next_log = load_log.get_log(next_index)

            res = self.do_append_entry(**{
                'leader_term' : curr_term,
                'leader_id' : id,
                'prev_log_idx' : prev_log.log_id,
                'prev_log_term' : prev_log.log_term,
                'worker_id' : None if (next_log is None) else next_log.worker_id,
                'worker_load' : None if (next_log is None) else next_log.worker_load,
                'commit_idx' : load_log.get_last_commited_id()
            })

            if res.success:
                if not (next_log is None):
                    next_index += 1
                else:
                    sleep(append_delay)
            else:
                next_index -= 1


class WorkerHandler(BaseHTTPRequestHandler):
    # The main load balancer server class

    def handle_request_vote(self, **kwargs):
        # Handle request vote RPC
        # kwargs should contains candidate_id, term, last_log_idx, last_log_term
        # TODO: Implement this method
        pass

    def handle_append_entry(self, **kwargs):
        # Handle append entry RPC
        # kwargs should contains leader_term, leader_id, prev_log_idx, prev_log_term, worker_id, worker_load, commit_idx
        # TODO: Implement this method
        pass

    def handle_worker_request(self, number):
        # Handle client request to access the worker (forward to the least busy worker)
        # TODO: Implement this method
        pass

    def do_GET(self):
        # Should be clear enough
        # TODO: Implement this method
        pass


def load_conf():
    # Method to load the conf.txt file, and write to workerhost and balancerhost

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


def get_port():
    # Get the desired port and the server ID from the conf

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
