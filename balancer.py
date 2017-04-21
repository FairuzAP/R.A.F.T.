#!/usr/bin/python

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from time import sleep
from threading import Thread
from requests import get
from sys import exit
from storage import workerLoad, loadLog, raftState
from random import seed, uniform
import json


# Timer constant definition
HEARTBEAT_DELAY = 0.1
RPC_TIMEOUT = 0.5
WORKER_TIMEOUT = 2.0
ELECTION_TIMEOUT = 2.0

# Global Variable definition
workerhost = []
balancerhost = []
verbose = True
server_id = 1

# Global variables containing server critical information
worker_load = workerLoad()  # The global worker load storage interface
load_log = loadLog()        # The global RAFT log storage interface
raft_state = raftState()    # The server's current term and voted_for information
state = 0                   # The state of the server, 0=Follower, 1=Candidate, 2=Leader


timeout_counter = True      # Wether or not the server had received words from leader since the last ELECTION_TIMEOUT
class TimeoutChecker(Thread):
    # Thread subclass that will handle the election timeout if the server's current state is NOT a leader.
    # Periodically will check the timeout_counter value and set it to False. The server's RPC will set the
    # timeout_counter back to True when receiving append_entries RPC.
    # If during the second check the timeout_counter value is still false, meaning that the leader haven't
    # contacted the server in a while. So the server will become a candidate
    # TODO: Not exactly the standard way to do election timeout, better to use event-thingy

    def __init__(self):
        global timeout_counter
        timeout_counter = True
        seed()
        Thread.__init__(self)

    def start_new_term(self):
        # Upgrade the current server to a candidate state,
        pass

    def run(self):
        global timeout_counter
        while True:

            # Randomize (to some extent) the election timeout
            next_timeout = ELECTION_TIMEOUT + uniform(-0.5,0.5)
            sleep(next_timeout)

            if state == 2: break
            if not timeout_counter: self.start_new_term()
            timeout_counter = False


worker_counter = []         # The state of all worker, used by worker_timeout
class Heartbeat(Thread):
    # The heartbeat daemon thread subclass; Will be summoned by a new leader, and killed if the owner stepped down
    # If a target's log is consistent with the leader's send empty heartbeat periodically Else, begin converting the
    # log into the leader's one by one (especially when a new record is appended, every other node will be inconsistent)
    # This way, the contains of load_log will automatically be broadcasted at any point

    def __init__(self):

        global worker_counter
        worker_counter = []
        self.node = []

        # Initate the balancer node array, and the worker_counter
        for i in range(balancerhost.__len__()):
            self.node.append({'next_idx' : load_log.get_size() + 1, 'last_commit' : None})
            worker_counter.append(False)

        # Start the timeout thread, and it's own thread
        self.timeout = Thread(target=self.worker_timeout)
        self.timeout.daemon = True
        self.timeout.start()
        Thread.__init__(self)

    def worker_timeout(self):
        # Will be initiated in form of thread at the init of Heartbeat daemon
        # Periodically check the state of global worker_counter; if the update_workload RPC hasn't set a worker state
        # to True of a particular worker during the timeout period (and the worker is still considered up), append log
        # to add information that the worker is considered down right now

        global raft_state, load_log, worker_load, worker_counter
        this_term = raft_state.get_term()
        sleep(WORKER_TIMEOUT)

        # While the term is still the same, with delay in between
        while this_term == raft_state.get_term():

            # Check if all worker had contacted the load balancer between the timeout,
            # If not, then the worker is presumed dead
            for i in range(balancerhost.__len__()):
                if not worker_counter[i]:
                    if worker_load.get_load(i) < 1000:
                        load_log.append_log(raft_state.get_term(), i, 1000)
                worker_counter[i] = False

            sleep(WORKER_TIMEOUT)

    def do_append_entry(self, balancer_id, **kwargs):
        # Do an append entry RPC to the supplied balancer host id
        # kwargs should contains leader_term, leader_id, prev_log_idx, prev_log_term, worker_id, worker_load, commit_idx
        # Return the results of 'success' and 'term' in form of dictionary. Or None if something bad happens

        if verbose: print("Sending append entry RPC to " + balancerhost[balancer_id])
        try:
            r = get(balancerhost[balancer_id] + "append/" + json.dumps(kwargs), timeout = RPC_TIMEOUT)
            result = json.loads(r.json())
        except Exception as e:
            if verbose: print(e)
            result = None

        return result

    def send_heartbeat(self, i):
        # Send a heartbeat to balancer node with id i, the content of the heartbeat is according to the next_idx
        # value of this node. Will also update the next_idx according to the res id the returned term is still
        # Consistent with the state of this server

        global raft_state, load_log
        prev_log = load_log.get_log(self.node[i].next_idx - 1)

        # Prepare all the necessary new log
        new_log = []
        for i in range(self.node[i].next_idx, load_log.get_size() + 1):
            next_log = load_log.get_log(i)
            new_log.append({'worker_id': next_log.worker_id, 'worker_load': next_log.worker_load})

        # Send the apropriate RPC according to the host log state
        res = self.do_append_entry(i, **{
            'leader_term': raft_state.get_term(),
            'leader_id': server_id,
            'prev_log_idx': prev_log.log_id,
            'prev_log_term': prev_log.log_term,
            'new_log': new_log,
            'commit_idx': load_log.get_last_commited_id()
        })

        # Modify the next index accordingly
        if res.success and (res.term <= raft_state.get_term()):
            self.node[i].next_idx += new_log.__len__()
            self.node[i].last_commit = self.node[i].next_idx - 1
        else:
            self.node[i].next_idx -= 1
        return res

    def update_log_commit(self):
        # Keep track of how many node commit each uncommited log entry. Commit each log if possible
        global worker_load, load_log
        for nextcommit in range(load_log.get_last_commited_id(), load_log.get_size()):
            commit = True

            for node in self.node:
                if node.last_commit is None:
                    commit = False
                    break
                if node.last_commit <= nextcommit:
                    commit = False
                    break

            if commit:
                load_log.commit_log(nextcommit, worker_load)
            else:
                break

    def run(self):
        # An infinite loop of spamming the target with append entry RPC if not consistent with leader
        # Or periodically send heartbeat if the target is consistent
        # The moment curr_term is changed, will terminate automatically
        # TODO: Currently, each request is blocking, making the system relatively un-scalable. Fix this. Later.

        global raft_state, state, worker_load, load_log
        this_term = raft_state.get_term()

        # While the term is still the same, with delay in between
        while this_term == raft_state.get_term():

            # For every other host other than this one,
            for i in range(balancerhost.__len__()):
                if i == server_id: continue

                try:
                    # Send the heartbeat to this host
                    res = self.send_heartbeat(i)

                    # If a host has a higher term, step down (not forgetting the election timeout)
                    if res.term > raft_state.get_term():
                        raft_state.set_term(res.term)
                        state = 0
                        timer = TimeoutChecker()
                        timer.daemon = True
                        timer.start()
                        break

                # If an error occur during RPC, continue on
                except Exception as e:
                    if verbose: print(e)
                    continue

            self.update_log_commit()
            sleep(HEARTBEAT_DELAY)


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

        global raft_state, state, timeout_counter

        # If the sender term is lower, return the correct term
        if kwargs.leader_term < raft_state.get_term():
            res = { 'success' : False, 'term' : raft_state.get_term() }
        else:

            # Replace the current term, if the sender's is higher
            if kwargs.leader_term > raft_state.get_term():
                raft_state.set_term(kwargs.leader_term)

            # TODO: If candidate or leader, step down here

            # "Resetting" the election timer
            timeout_counter = True

            # Check the log consistency, return failed RPC if not consistent
            prev_log = load_log.get_log(kwargs.prev_log_idx)
            if prev_log is None:
                res = { 'success': False, 'term': raft_state.get_term() }
            else:
                if prev_log.log_term != kwargs.prev_log_term:
                    res = {'success': False, 'term': raft_state.get_term()}
                else:

                    # Check if the RPC contains new log to record, and append it if exist
                    for item in kwargs.log:
                        load_log.replace_log(kwargs.prev_log_idx + 1, kwargs.prev_log_term, item.worker_id, item.worker_load)

                    # Commit all the committed log
                    for i in range(kwargs.commit_idx, load_log.get_last_commited_id()):
                        if not load_log.is_commited(i):
                            load_log.commit_log(i,worker_load)

                    res = {'success': True, 'term': raft_state.get_term()}

        self.send_response(200)
        self.end_headers()
        self.wfile.write(str(json.loads(res)).encode('utf-8'))

    def handle_worker_request(self, number):
        # Handle client request to access the worker (forward to the least busy worker)

        global worker_load

        worker_id = worker_load.get_idle_worker()
        url = workerhost[worker_id] + number.__str__()
        r = get(url, timeout=RPC_TIMEOUT)

        self.send_response(200)
        self.end_headers()
        self.wfile.write(str(r.text).encode('utf-8'))

    def handle_update_workload(self, work_id, load):
        # Handle workload broadcast from the worker nodes

        global load_log, worker_load, raft_state, state

        # Return if not the current leader
        if state == 2:

            global worker_counter
            worker_counter[work_id] = True

            # Check if the difference is significant
            old_load = worker_load.get_load(work_id)
            if abs(old_load - load) > 5:
                load_log.append_log(raft_state.get_term(), work_id, load)

        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        # Should be clear enough

        try:
            args = self.path.split('/')

            if args[1] == 'append':
                dict = json.loads(args[2])
                self.handle_append_entry(**dict)

            elif args[1] == 'load':
                dict = json.loads(args[2])
                self.handle_update_workload(dict.worker_id, dict.worker_load)

            elif args[1] == 'vote':
                dict = json.loads(args[2])
                self.handle_request_vote(**dict)

            else: self.handle_worker_request(int(args[1]))

        except Exception as e:
            if verbose: print(e)
            self.send_response(500)
            self.end_headers()


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
