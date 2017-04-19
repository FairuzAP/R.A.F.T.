#!/usr/bin/python

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from time import sleep
from os import getpid
from threading import Thread
from requests import get
from sys import exit
from storage import workerLoad, loadLog
import json


# Global Variable definition
workerhost = []
balancerhost = []
append_delay = 0.1
verbose = True
server_id = 1


# Global variables containing server critical information
worker_load = workerLoad()  # The global worker load storage interface
load_log = loadLog()        # The global RAFT log storage interface
state = 0                   # The state of the server, 0=Follower, 1=Candidate, 2=Leader
leader_heart = None         # Will contains the handler to heartbeat thread if the state is the leader
# TODO: Move the variables below into being stored in a stable storage
curr_term = 0               # The server's current Term
voted_for = None            # The server id that this server voted for this term


class Heartbeat(Thread):
    # The heartbeat daemon thread subclass; Will be summoned by a new leader, and killed if the owner stepped down
    # If a target's log is consistent with the leader's send empty heartbeat periodically Else, begin converting the
    # log into the leader's one by one (especially when a new record is appended, every other node will be inconsistent)
    # This way, the contains of load_log will automatically be broadcasted at any point

    def __init__(self):

        self.next_idx = []
        for i in range(balancerhost.__len__()-1):
            self.next_idx.append(load_log.get_size() + 1)

        Thread.__init__(self)

    def do_append_entry(self, balancer_id, **kwargs):
        # Do an append entry RPC to the supplied balancer host id
        # kwargs should contains leader_term, leader_id, prev_log_idx, prev_log_term, worker_id, worker_load, commit_idx
        # Return the results of 'success' and 'term' in form of dictionary. Or None if something bad happens

        try:
            r = get(balancerhost[balancer_id] + "append/" + json.dumps(kwargs), timeout = 0.1)
            result = json.loads(r.json())
        except Exception as e:
            if verbose:
                print(e)
            result = None

        return result

    def run(self):
        # An infinite loop of spamming the target with append entry RPC if not consistent with leader
        # Or periodically send heartbeat if the target is consistent
        # The moment curr_term is changed, will terminate automatically
        # TODO: Currently, each request is blocking, making the system un-scalable. Fix this. Later.

        global curr_term, state
        this_term = curr_term

        # While the term is still the same, with delay in between
        while this_term == curr_term:
            sleep(append_delay)

            # For every other host other than this one,
            for i in range(balancerhost.__len__()-1):
                if i == server_id:
                    continue

                prev_log = load_log.get_log(self.next_idx[i]-1)

                # Prepare all the necessary new log
                new_log = []
                for i in range(self.next_idx[i], load_log.get_size()):
                    next_log = load_log.get_log(i)
                    new_log.append({'worker_id' : next_log.worker_id, 'worker_load' : next_log.worker_load})

                # Send the apropriate RPC according to the host log state
                res = self.do_append_entry( i, **{
                    'leader_term' : curr_term,
                    'leader_id' : server_id,
                    'prev_log_idx' : prev_log.log_id,
                    'prev_log_term' : prev_log.log_term,
                    'new_log' : new_log,
                    'commit_idx' : load_log.get_last_commited_id()
                })

                # If an error occur during RPC, continue on
                if res is None:
                    continue

                # If a host has a higher term, step down
                if res.term > curr_term:
                    curr_term = res.term
                    state = 0
                    break

                # Else, modify the next index accordingly
                if res.success:
                    self.next_idx[i] += new_log.__len__()
                    # TODO: Keep track of how many nodes has commit each log entry, so that.. you know.. stuffs
                else:
                    self.next_idx[i] -= 1


class BalancerHandler(BaseHTTPRequestHandler):
    # The main load balancer server class

    def handle_request_vote(self, **kwargs):
        # Handle request vote RPC
        # kwargs should contains candidate_id, term, last_log_idx, last_log_term
        # TODO: Implement this method
        pass

    def handle_append_entry(self, **kwargs):
        # Handle append entry RPC
        # kwargs should contains leader_term, leader_id, prev_log_idx, prev_log_term, log, commit_idx

        global curr_term, state

        # If the sender term is lower, return the correct term
        if kwargs.leader_term < curr_term:
            res = { 'success' : False, 'term' : curr_term }
        else:

            # Replace the current term, if the sender's is higher
            if kwargs.leader_term > curr_term:
                curr_term = kwargs.leader_term

            # TODO: If candidate or leader, step down here
            # TODO: Reset election timer here

            # Check the log consistency, return failed RPC if not consistent
            prev_log = load_log.get_log(kwargs.prev_log_idx)
            if prev_log is None:
                res = { 'success': False, 'term': curr_term }
            else:
                if prev_log.log_term != kwargs.prev_log_term:
                    res = {'success': False, 'term': curr_term}
                else:

                    # Check if the RPC contains new log to record, and append it if exist
                    for item in kwargs.log:
                        load_log.replace_log(kwargs.prev_log_idx + 1, kwargs.prev_log_term, item.worker_id, item.worker_load)

                    # Commit all the committed log
                    for i in range(kwargs.commit_idx, load_log.get_last_commited_id()):
                        if not load_log.is_commited(i):
                            load_log.commit_log(i,worker_load)

                    res = {'success': True, 'term': curr_term}

        self.send_response(200)
        self.end_headers()
        self.wfile.write(str(json.loads(res)).encode('utf-8'))

    def handle_worker_request(self, number):
        # Handle client request to access the worker (forward to the least busy worker)
        # TODO: Implement this method
        pass

    def do_GET(self):
        # Should be clear enough
        # TODO: Implement the url parser, and make the url pattern for the above RPC
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

    global server_id
    print("There seems to be " +balancerhost.__len__().__str__()+ " balancer hosts in the system:")
    for host in balancerhost:
        print(server_id.__str__() +". "+ host.__str__())
        server_id += 1

    try:
        server_id = int(input("Which one am I supposed to be? ")) - 1
        start = balancerhost[server_id].find(":", 8)
        end = balancerhost[server_id].find("/", start)
        return int(balancerhost[server_id][start+1:end])
    except:
        print("Error in parsing port number")
        exit()


def main():
    load_conf()

    # Get the desired port and prepare the server
    current_port = get_port()
    print(current_port)

    # TODO: Implement this method so that the program can actually start


if __name__ == "__main__": main()
