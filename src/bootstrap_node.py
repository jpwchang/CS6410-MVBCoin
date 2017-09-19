################################################################################
#
# bootstrap_node.py
#
# Runs the bootstrap node. The bootstrap node listens for incoming requests and
# broadcasts all known nodes to the requester
#
################################################################################

import socket
import select

from utils import *

def bootstrap_node():
    print("Starting bootstrap node!")
    # keep track of the known ports
    known_ports = set()
    known_conns = set()

    # socket to handle incoming connections
    boot_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    boot_sock.bind(("localhost", 8888))
    boot_sock.listen(10)

    conn_added = False

    # listen for incoming connections
    while True:
        readable_conns, writable_conns, _ = select.select([boot_sock], list(known_conns), [])
 
        if conn_added:
            # construct a message with the number of known ports followed by
            # each of those ports
            msg = b'4' + int_to_bytes(len(known_ports), 6)
            for known_port in known_ports:
                msg += known_port

            # send the message to all known ports
            for conn in writable_conns:
                conn.sendall(msg)
            conn_added = False
            
        for conn in readable_conns:
            if conn == boot_sock:
                # if the server socket is available, this means there is a new
                # incoming connection. Accept the connection and add it to the
                # list of connections that can be written to
                incoming_conn, addr = conn.accept()
                print("Received connection from", addr)
                # fetch the sent data. We expect 38 bytes.
                data = incoming_conn.recv(38)
                print("Request data:", data)
                # the first 6 bytes should be the port
                port = data[:6]
                # add the port to the list of known ports
                known_ports.add(port)
                # add the connection to the list of writable connections
                known_conns.add(incoming_conn)
                conn_added = True
