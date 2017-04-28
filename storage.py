#!/usr/bin/python
import pickle


class workerLoad():
    # Interface to store every worker load and up/down information,
    # Can volatile for debugging; MUST be implemented into stable storage later
	# Simpan tabel beban pekerja

    def __init__(self, nWorker, server_id):
        self.nWorker  = nWorker
        self.workerload = [0] * (nWorker);
        self.server_id = server_id
        self.persistent_save_init()

    def persistent_save_init(self):
        self.filename = str(self.server_id) + "_workerload.txt"
        try:
            with open(self.filename, 'rb') as filedata:
                self.nWorker = pickle.load(filedata)
                self.workerload = pickle.load(filedata)
        except Exception:
            self.file = open(self.filename, 'w')
        print(self.filename)
    

    def persistent_save(self):
        with open(self.filename, 'wb') as filedata:
            pickle.dump(self.nWorker,filedata, pickle.HIGHEST_PROTOCOL)
            pickle.dump(self.workerload , filedata , pickle.HIGHEST_PROTOCOL)

    def set_load(self, id, load):
        # Set the load of the appropriate worker, Load of 1000 means the node is down for now
        self.workerload[id] = load;
        self.persistent_save()
       
        
    def get_load(self, id):
        # Return the current load of the worker with the supplied id
        return self.workerload[id]

    def print_worker_load(self):
        for i in range(0,self.nWorker):
            print (str(i)+"-" + str(wload.get_load(i)))
            pass


    def get_idle_worker(self):
        # Return the id of the most idle worker host
        min_idx = 0
        min_val = self.workerload[min_idx]
        for i in range(1 , self.nWorker):
            if (self.get_load(i) < min_val):
                min_idx = i
                min_val = self.get_load(i)

        return min_idx


class logTuple:
    def __init__(self,term,idworker,value):
        self.term = term
        self.idworker = idworker
        self.value = value

    def modify(self,term,idworker,value):
        self.term = term
        self.idworker = idworker
        self.value = value


class loadLog():
    # Interface to store the RAFT log, where each "act" is an invocation of the workerLoad set_load method
    # Can volatile for debugging; MUST be implemented into stable storage later
	# <ID, Term, IDworker yang diubah, nilai terbaru>
	# Pointer ke tuple mana 

    def __init__(self, server_id):
        self.commitedLog = -1
        self.nLog = -1
        self.log = []
        self.server_id = server_id
        self.persistent_save_init()


    def persistent_save_init(self):
        self.filename = str(self.server_id) + "_loadlog.txt"
        try:
            with open(self.filename, 'rb') as filedata:
                self.commitedLog = pickle.load(filedata)
                self.nLog = pickle.load(filedata)
                self.log = pickle.load(filedata)
        except Exception:
            self.file = open(self.filename, 'w')
        print(self.filename)

    def persistent_save(self):
        with open(self.filename, 'wb') as filedata:
            pickle.dump(self.commitedLog,filedata, pickle.HIGHEST_PROTOCOL)
            pickle.dump(self.nLog , filedata , pickle.HIGHEST_PROTOCOL)
            pickle.dump(self.log , filedata , pickle.HIGHEST_PROTOCOL)

    def append_log(self, term, worker_id, worker_load):
        # Append the log with the supplied data and return true if consistent, else return false
        logItem = logTuple(term,worker_id,worker_load)
        self.log.append(logItem)
        self.nLog += 1
        self.persistent_save()
		
    def replace_log(self, log_id, term, worker_id, worker_load):
        # Replace the log with the supplied data and return true if consistent, else return false
        # If the id is in the middle, then delete all the following log
        # Kalau log_id gak ada di log, appendlah..
		# Log_id ke sekian, ubah dengan yg diminat.. terus atasnya hapus..
        # Yang sudah dicommit gak boleh direplace
        # Log id 
        if (log_id == (self.nLog + 1)):
            self.append_log(term,worker_id,worker_load)
        elif (log_id > (self.nLog + 1)):
            raise Exception('Log_ID is bigger than current Log_ID + 1')
        elif ( log_id < self.commitedLog):
            raise Exception('Trying to replace committed log')
        else:
            self.log[log_id].modify(term,worker_id,worker_load)
            for i in range(log_id+1 , self.nLog):
                #print "pop"
                self.log.pop()
            self.nLog = log_id+1
            self.persistent_save()


    def print_log(self):
        #print "nLog : " + str(self.nLog)
        #print "commitedLog : " + str(self.commitedLog)
        for i in range(0, self.nLog):
            #print str(i) + " Term : " + str(self.log[i].term) + " WorkerID : " + str(self.log[i].idworker) + "->"+ str(self.log[i].value)
            pass


    def get_log(self, log_id):
        # Return the log object in this id in form of a dictionary; None if doesn't exist
        if log_id == -1:
            return {
                'log_id' : -1,
                'log_term' :  0,
                'worker_id' : -1,
                'worker_load' : -1
            }

        elif(log_id > self.nLog):
            return None

        else:
            return {
                'log_id' : log_id ,
                'log_term' :  self.log[log_id].term,
                'worker_id' : self.log[log_id].idworker,
                'worker_load' : self.log[log_id].value
            }

    def commit_log(self, log_id, worker_load):
        # Mark the supplied id log as committed, and pass the changes instructed to the workerLoad object
        # If the supplied id is already committed, then return true
        # If id-1 hasn't been commited yet, then something BAD is going on
		# log_id ke x di commit beneran di worker_load..
		# Throw exception jika ada masalah..
        # Id gak ada -> x
        # Log ini sudah dicommit -> x
        # Commit log yang gak kontinu -> x

        if ( log_id > (self.nLog)):
            raise Exception('Specified LogID not found in log')
        elif ( log_id <= self.commitedLog):
            raise Exception('Specified LogID is already committed')
        elif ( log_id > self.commitedLog+1 ):
            raise Exception('Skipped commit')
        else:
            target_worker = self.log[log_id].idworker
            target_value = self.log[log_id].value
            worker_load.set_load(target_worker,target_value)
            self.commitedLog = log_id
            self.persistent_save()

    def is_commited(self, log_id):
        # Return wether or not the log with the supplied id is commited
        if (log_id > self.commitedLog):
            result = False
        else:
            result = True
        return result

    def get_size(self):
        # Return the last log id
        return self.nLog

    def get_last_commited_id(self):
        return self.commitedLog


class raftState():
    # Stable storage interface for storing server's term and voted_for information
    # term, voted for..
    def __init__(self, server_id):
        self.term = 0
        self.votedFor = None
        self.server_id = server_id
        self.persistent_save_init()

    def persistent_save_init(self):
        self.filename = str(self.server_id) + "_raftState.txt"
        try:
            with open(self.filename, 'rb') as filedata:
                self.term = pickle.load(filedata)
                self.votedFor = pickle.load(filedata)
        except Exception:
            self.file = open(self.filename, 'w')
        print(self.filename)

    def persistent_save(self):
        with open(self.filename, 'wb') as filedata:
            pickle.dump(self.term,filedata, pickle.HIGHEST_PROTOCOL)
            pickle.dump(self.votedFor , filedata , pickle.HIGHEST_PROTOCOL)

    def get_term(self):
        return self.term

    def get_voted_for(self):
        return self.votedFor

    def set_term(self, term):
        # For security and stuff, also set voted_for to None during term change
        if (term < self.term):
            raise Exception('New term is lower than old term')
        else:
            self.term = term
            self.votedFor = None
            self.persistent_save()
        
    def set_voted_for(self, vote):
        self.votedFor = vote
        self.persistent_save()





wload = workerLoad(9,2)
for i in range(0,9):
    wload.set_load(i,50-(i*2))
    print (str(i)+"-" + str(wload.get_load(i)))
print ("Ternanggur" +  str(wload.get_idle_worker()))

log = loadLog(2)
log.append_log(1,0,90)
log.append_log(1,1,50)
log.append_log(1,1,60)
log.append_log(1,1,80)
log.print_log()
log.replace_log(1,1,1,990)
log.print_log()
print (log.get_log(0))
log.commit_log(0,wload)
log.print_log()
wload.print_worker_load()
print (log.is_commited(0))
print (log.get_size())
print (log.get_last_commited_id())

raftstate = raftState(5)
print ("raftstate")
print (raftstate.get_term())
print (raftstate.get_voted_for())

raftstate.set_term(1)
print (raftstate.get_term())
print (raftstate.get_voted_for())
raftstate.set_voted_for(2)
print (raftstate.get_voted_for())


