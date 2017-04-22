#!/usr/bin/python

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
        # Set the load of the appropriate worker, Load of 1000 means the node is down for now
        pass

    def get_load(self, id):
        # Return the current load of the worker with the supplied id
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
        # Return the last log id
        pass

    def get_last_commited_id(self):
        pass


class raftState():
    # Stable storage interface for storing server's term and voted_for information
    # TODO: Implement this class

    def get_term(self):
        pass

    def get_voted_for(self):
        pass

    def set_term(self, term):
        # For security and stuff, also set voted_for to None during term change
        pass

    def set_voted_for(self, vote):
        pass

