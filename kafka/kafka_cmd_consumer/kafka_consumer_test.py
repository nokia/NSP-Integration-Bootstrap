#!/usr/bin/env python
import threading, logging, time
import multiprocessing
import sys
from kafka import KafkaConsumer
import json
"""Collect command-line options in a dictionary"""
TERMINAL_WIDTH = 0
TERMINAL_HEIGHT = 0

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# print bcolors.WARNING + "Warning: No active frommets remain. Continue?" + bcolors.ENDC



def getopts(argv):
    opts = {}  # Empty dictionary to store key-value pairs.
    while argv:  # While there are arguments left to parse...
        if argv[0][0] == '-':  # Found a "-name value" pair.
            opts[argv[0]] = argv[1]  # Add key and value to the dictionary.
        argv = argv[1:]  # Reduce the argument list by copying it starting from index 1.
    return opts

def getTerminalSize():
    import os
    env = os.environ
    def ioctl_GWINSZ(fd):
        try:
            import fcntl, termios, struct, os
            cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,
        '1234'))
        except:
            return
        return cr
    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        cr = (env.get('LINES', 25), env.get('COLUMNS', 80))

        ### Use get(key[, default]) instead of a try/catch
        #try:
        #    cr = (env['LINES'], env['COLUMNS'])
        #except:
        #    cr = (25, 80)
    return int(cr[1]), int(cr[0])

class Consumer(multiprocessing.Process):
    host_id = ''
    topic_id = ''

    def __init__(self, host, topic):
        self.host_id = str(host)
        self.topic_id = topic
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        
    def stop(self):
        self.stop_event.set()
        
    def run(self):
        last= 0
        consumer = KafkaConsumer(bootstrap_servers=self.host_id,
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe([self.topic_id])

        while not self.stop_event.is_set():
            for message in consumer:
                # print(message.value)
                # print "printing + " + str(TERMINAL_WIDTH)
                # print bcolors.WARNING + ("*" * (TERMINAL_WIDTH)) + bcolors.ENDC
                msg = json.loads(message.value)
                print json.dumps(msg, indent=4, sort_keys=True)
                
                print bcolors.WARNING + ("-" * (TERMINAL_WIDTH)) + bcolors.ENDC
            if self.stop_event.is_set():
                break

        consumer.close()
        


def main():
    from sys import argv
    myargs = getopts(argv)
    HOST_IP=''
    TOPIC=''

    if '-host' in myargs:  # Example usage.
        HOST_IP = myargs['-host']
    else:
        sys.exit('Please use as follows python kafka_test_consumer.py -host HostIP -topic TOPIC');

    if '-topic' in myargs:  # Example usage.
        TOPIC = myargs['-topic']
    else:
        sys.exit('please use as follows python kafka_test_consumer.py -host HostIP -topic TOPIC');
    print(myargs)

    tasks = [
        Consumer(HOST_IP,TOPIC)
    ]

    for t in tasks:
        t.start()

    for task in tasks:
        task.join()
        
        
if __name__ == "__main__":
    (TERMINAL_WIDTH, TERMINAL_HEIGHT) = getTerminalSize()
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
