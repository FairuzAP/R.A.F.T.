#!/usr/bin/python

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from time import sleep
from os import getpid
from threading import Thread
from requests import get
from sys import exit
import psutil
import json


# Timer constant definition
DAEMON_DELAY = 5
RPC_TIMEOUT = 0.5

# Global Variable definition
workerhost = []
balancerhost = []
server_id = 1
pid = getpid()
verbose = False


def load_conf():
    # Method to load the conf.txt file, and write to workerhost and balancerhost

    global workerhost, balancerhost

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

    print("There seems to be " +workerhost.__len__().__str__()+ " worker hosts in the system:")
    for host in workerhost:
        print(server_id.__str__() + ". " + host.__str__())
        server_id += 1

    try:
        server_id = int(input("Which one am I supposed to be? ")) - 1
        start = workerhost[server_id].find(":", 8)
        end = workerhost[server_id].find("/", start)
        return int(workerhost[server_id][start + 1:end])
    except:
        print("Error in parsing port number")
        exit()


def get_workload():
    # Get the current main thread workload

    return psutil.Process(pid).cpu_percent(interval=DAEMON_DELAY)


def worker_daemon_method():
    # Periodically broadcast workload to all balancerhost

    while(True):
        current_workload = get_workload().__str__()
        print("Broadcasting current workload of " + current_workload)

        for url in balancerhost:
            load = {
                'worker_id' : server_id,
                'worker_load' : current_workload,
            }
            t = SendWorkload(url + "load/" + json.dumps(load), RPC_TIMEOUT)
            t.start()

        sleep(DAEMON_DELAY)


class WorkerHandler(BaseHTTPRequestHandler):
    # The main worker server class

    def prime(self, n):
        i = 2
        while i * i <= n:
            if n % i == 0:
                return False
            i += 1
        return True

    def calc(self, n):
        p = 1
        while n > 0:
            p += 1
            if self.prime(p):
                n -= 1
        return p

    def do_GET(self):
        try:
            args = self.path.split('/')
            if len(args) != 2:
                raise Exception()
            n = int(args[1])
            self.send_response(200)
            self.end_headers()
            self.wfile.write(str(self.calc(n)).encode('utf-8'))
        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)


class SendWorkload(Thread):
    # Thread class that will be used to send

    def __init__(self, url, timeout):
        Thread.__init__(self)
        self.url = url
        self.timeout = timeout

    def run(self):
        try:
            get(self.url, timeout = self.timeout)
        except Exception as e:
            if verbose:
                print("Workload broadcast failed for " + self.url)
        return


def main():
    load_conf()

    # Get the desired port and prepare the server
    current_port = get_port()
    try:
        worker = HTTPServer(("", current_port), WorkerHandler)
        worker_thread = Thread(target=worker.serve_forever)
        worker_thread.daemon = True
    except:
        print("Error in starting worker server")
        exit()
    print("Worker Server " + server_id.__str__() + " Running at port " + current_port.__str__())

    # Start the server daemon and the server
    worker_daemon = Thread(target=worker_daemon_method)
    worker_daemon.daemon = True
    worker_daemon.start()
    worker_thread.start()

    input("\nPress anything to exit..\n\n")


if __name__ == "__main__": main()

