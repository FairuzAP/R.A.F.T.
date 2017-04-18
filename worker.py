#!/usr/bin/python

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from time import sleep
from os import getpid
import psutil
import requests
import sys
import threading


# Global Variable definition
workerhost = []
balancerhost = []
daemon_delay = 0.5
pid = getpid()


# The main worker server class
class WorkerHandler(BaseHTTPRequestHandler):

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


# Method to load the conf.txt file, and write to workerhost and balancerhost
def load_conf():
    # Try opening the config file
    try:
        conf = open("conf.txt","r",1)
    except IOError:
        print("Error, conf.txt file not found")
        sys.exit()

    # Set the worker and loadbalancer host list
    workercount = int(conf.__next__())
    for index in range(workercount):
        workerhost.append(conf.__next__().rstrip())
    balancercount = int(conf.__next__())
    for index in range(balancercount):
        balancerhost.append(conf.__next__().rstrip())

    # Close the config file
    conf.close()
    print("Config file loaded")


# Get the current main thread workload
def get_workload():
    return psutil.Process(pid).cpu_percent(interval=1.0)


# Periodically broadcast workload to all balancerhost
def worker_daemon_method():
    while(True):
        current_workload = get_workload().__str__()
        print("Current workload is " + current_workload)

        for url in balancerhost:
            try:
                r = requests.get(url + "load/" + current_workload, timeout=0.001)
            except Exception as e:
                print("Workload broadcast failed for " + url)
                pass
        sleep(daemon_delay)


def main():
    load_conf()

    # Get the desired port and prepare the server
    current_port = int(input("Which port will this worker run at?\n"))
    worker = HTTPServer(("", current_port), WorkerHandler)

    # Start the server daemon and the server
    worker_daemon = threading.Thread(target=worker_daemon_method)
    worker_daemon.daemon = True
    worker_daemon.start()
    worker.serve_forever()


if __name__ == "__main__": main()

