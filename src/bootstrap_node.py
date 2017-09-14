################################################################################
#
# bootstrap_node.py
#
# Runs the bootstrap node. The bootstrap node listens for incoming requests and
# broadcasts all known nodes to the requester
#
################################################################################

import socket

from utils import *

def bootstrap_node():
    print("Starting bootstrap node!")
    # keep track of the known ports
    known_ports = set()

    # socket to handle incoming connections
    boot_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    boot_sock.bind(("localhost", 8888))
    boot_sock.listen(10)

    # listen for incoming connections
    while True:
        conn, addr = boot_sock.accept()
        print("Received connection from", addr)
        # fetch the sent data. We expect 38 bytes.
        data = conn.recv(38)
        print("Request data:", data)
        # the first 6 bytes should be the port
        port = data[:6]
        # add the port to the list of known ports
        if port not in known_ports:
            known_ports.add(port)

        # construct a message with the number of known ports followed by
        # each of those ports
        msg = b'4' + int_to_bytes(len(known_ports), 6)
        for known_port in known_ports:
            msg += known_port

        # reply to the new node with the message
        conn.sendall(msg)

        # close the other end's connection (our implementation does
        # not use a persistent connection for communication to other
        # nodes.
        conn.close()

        # send the message to all known ports OTHER than the newly
        # connected one (which already received our reply)
        for known_port in known_ports:
            if known_port == port:
                continue
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(("localhost", int(known_port)))
                sock.sendall(msg)
                sock.close()


