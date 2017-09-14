import hashlib
import socket
import binascii
import select
import threading
import random
from collections import Counter
from queue import Queue

from utils import *
from block import Block
from blockchain import Blockchain

# Constants
NODE_ADDRESS = hashlib.sha256(b"jpc362").digest()
BOOTSTRAP_PORT = 8888
OPCODE_TRANSACTION = b'0'
OPCODE_BLOCK = b'1'
OPCODE_GETBLOCK = b'2'
OPCODE_GETHASH = b'3'
OPCODE_PORTS = b'4'
NUM_TX_MEMPOOL = 50000
BLOCK_MSG_LEN = 160 + 128 * NUM_TX_MEMPOOL
# connections states
CONN_STATE_READY = 0
CONN_STATE_READ_BLOCK = 1

# the currently known height of the blockchain
cur_block_height = 0
# The UTXO set stores all known addresses that can spend money
utxo = Counter()
# The mempool stores all transactions that have not yet been committed
mempool = []
# the hash of the last block we mined or received
prev_hash = hashlib.sha256(b'0').digest()
# the blockchain itself
bc = Blockchain()

class Miner(threading.Thread):
    """
    Thread that performs block mining in the background, allowing the
    main thread to continue listening for messages
    """
    def __init__(self, block, interrupt_event, finish_event, result_q, 
                 group=None, target=None, name=None, args=(), kwargs={}, daemon=None):
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)
        self.block = block
        self.interrupt_event = interrupt_event
        self.finish_event = finish_event
        self.result_q = result_q

    def run(self):
        print("Miner thread started!")
        nonce_seed = 0
        block_hash = hashlib.sha256(self.block.as_bytearray())
        while not self.interrupt_event.is_set():
            # keep looping until we find a nonce that works, OR we get
            # interrupted by an incoming block
            nonce = int_to_bytes(nonce_seed, 32)
            nonced_hash = block_hash.copy()
            nonced_hash.update(nonce)
            h = nonced_hash.digest()
            if h[0] == 48 and h[1] == 48 and h[2] == 48:
                # we found a working nonce!
                print("[MINER] Nonce found!")
                self.result_q.put((nonce, h))
                self.finish_event.set()
                # the miner thread is done
                return
            nonce_seed += 1
        print("[MINER] Interrupted by incoming block!")
        self.interrupt_event.clear()

def get_known_ports(my_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # send port number concatenated with this node's address
        msg = int_to_bytes(my_port) + NODE_ADDRESS
        
        sock.bind(("localhost", my_port))
        sock.connect(("localhost", BOOTSTRAP_PORT))
        sock.sendall(msg)
        # now wait for the reply from the bootnode
        # since we don't know how many ports there will be the length of the
        # reply will be unknown
        reply = sock.recv(2048)
        print("Received reply", reply)
    # ignore the first 7 bytes (opcode and num_ports) and parse the ports
    return parse_ports(reply[7:])

def parse_ports(msg):
    # extract the list of ports from the reply
    ports = []
    for i in range(0, len(msg), 6):
        ports.append(int(msg[i:i+6]))
    print(ports)
    return ports

def parse_transaction(msg):
    sender = msg[:32]
    receiver = msg[32:64]
    amt = int(msg[64:96])
    timestamp = int(msg[96:])
    return sender, receiver, amt, timestamp

def handle_transaction(ext_conn, writers):
    """
    Handle a new transaction being sent over the external connection ext_conn.
    """
    transaction = ext_conn.recv(128)
    print("Received transaction")
    # don't repeat process out this transaction if we have already seen it
    if transaction in mempool:
        return
    sender, receiver, amt, timestamp = parse_transaction(transaction)
    print("From:", binascii.hexlify(sender))
    print("To:", binascii.hexlify(receiver))
    print("Amount:", amt)
    # check if the transaction is valid (sender must have enough coins
    # according to the UTXO set)
    if amt <= utxo[sender]:
        print("Sender has enough MVBCoin, transaction is valid")
        # propagate the transaction
        for writer in writers:
            writer.sendall(b'0' + transaction)
        # queue up the transaction in the mempool
        mempool.append(transaction)
        # update the balances of the sender and receiver
        utxo[sender] -= amt
        utxo[receiver] += amt
    else:
        print("Transaction invalid; sender has %d MVBCoin but tried to send %d. Skipping..." % (utxo[sender], amt))

def parse_block(block_bytes):
    """
    Parse a block sent over a socket as raw bytes
    """
    nonce = block_bytes[0:32]
    prior_hash = block_bytes[32:64]
    block_hash = block_bytes[64:96]
    block_height = int(block_bytes[96:128])
    block_miner_addr = block_bytes[128:160]
    block_data = block_bytes[160:]
    print("Finished receiving block from node at address", block_miner_addr, "with height", block_height)
    return nonce, prior_hash, block_hash, block_height, block_miner_addr, block_data

def update_ports(ext_conn):
    """
    Handle a port list update being sent over the external connection ext_conn
    """
    num_conns = int(ext_conn.recv(6))
    return parse_ports(ext_conn.recv(num_conns * 6))

def update_writers(port_list, writers, own_port):
    """
    Given a list of ports, update the collection of write sockets so that there
    is a writer for every port in the list
    """
    for port in port_list:
        if port not in writers and port != own_port:
            writers[port] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            writers[port].connect(("localhost", port))
            writers[port].setblocking(0)
    return writers

def write_block(block_bytes, ext_conn):
    """
    Send a block over an external connection
    """
    while len(block_bytes) > 0:
        _, writable_conns, _ = select.select([], [ext_conn], [])
        if len(writable_conns) == 1:
            bytes_written = writable_conns[0].send(block_bytes)
            block_bytes = block_bytes[bytes_written:]

def send_block(writers, block, nonce, block_hash):
    """
    Send a block that we have mined out to all other nodes
    """
    # get the byte representation of the block
    block_bytes = block.as_bytearray()
    # create a buffer to store our message
    msg = bytearray(161 + 128 * NUM_TX_MEMPOOL)
    # the first byte is the opcode, which is 1 (ASCII = 49)
    msg[0] = 49
    # the next 32 bytes are the nonce, which we mined
    msg[1:33] = nonce
    # next is the prior hash, which we can get from the block
    msg[33:65] = block.prev_hash
    # next is the hash, which we also mined
    msg[65:97] = block_hash
    # finally, the block height, miner address, and block data can be extracted
    # from the byte representation of the block
    msg[97:] = block_bytes[32:]
    print("Sending message with length", len(msg))
    for writer in writers:
        write_block(msg, writer)

def miner_node(num_workers, port, verbose=False):
    if verbose:
        print("Running", num_workers, "workers on ports", ports)

    # initialize the UTXO set
    for i in range(100):
        utxo[hashlib.sha256(bytes(str(i), encoding='ascii')).digest()] = 100000

    # connections to other nodes, indexed by their ports
    writers = {}

    # send a message to the bootnode informing that we are ready,
    # and get back the list of known ports
    known_ports = get_known_ports(port)
    # create new connections for the known ports
    writers = update_writers(known_ports, writers, port)

    # create a socket on which to listen to other nodes
    node_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_sock.bind(("localhost", port))
    node_sock.listen(10)

    # list of connections that could receive new messages. This includes
    # our own socket (which could receive an incoming connection) and
    # any currently active external connections
    read_conns = [node_sock]

    # synchronization primitives for communicating with miner thread
    # used to stop the mining when a new block is received
    interrupt_event = threading.Event()
    # used by the miner thread to signal when it is done mining
    finish_event = threading.Event()
    # used by the miner thread to pass back the nonce it found
    nonce_q = Queue()

    unmined_block = None

    global prev_hash, cur_block_height

    # for longer messages, it may take multiple recv's to receive a full message,
    # so to handle this we keep track of a state variable for each connection
    # that alters the behavior
    conn_state = {conn: CONN_STATE_READY for conn in read_conns + list(writers.values())}
    # number of bytes left to read for a block on a given connection
    bytes_remaining = {conn: 0 for conn in read_conns + list(writers.values())}
    # the incomplete block being read on a given connection
    conn_block = {}
    # the incomplete block being written over a given connection

    # flag to indicate whether the miner thread is active
    currently_mining = False

    # listen for messages from other nodes
    while True:
        conns_to_read, conns_to_write, _ = select.select(read_conns, list(writers.values()), [])
        for conn in conns_to_read:
            if conn == node_sock:
                # if the server socket is available, this means there is a new
                # incoming connection. Accept the connection and add it to the
                # list of connections that can be read
                incoming_conn, addr = conn.accept()
                read_conns.append(incoming_conn)
                conn_state[incoming_conn] = CONN_STATE_READY
                bytes_remaining[incoming_conn] = 0
            else:
                # this must be a new message from a previously connected node
                if conn_state[conn] == CONN_STATE_READY:
                    # first, read the opcode so we know how much data to chew
                    msg_type = conn.recv(1)

                    # separate handlers for each type of message
                    if msg_type == OPCODE_TRANSACTION:
                        handle_transaction(conn, conns_to_write)
                        # if the mempool now contains at least 50000 transactions,
                        # AND the miner thread is not busy, add them to a block and mine
                        if len(mempool) >= NUM_TX_MEMPOOL and not currently_mining:
                            # create a new block for mining
                            print("Mining new block at height", cur_block_height)
                            unmined_block = Block(NUM_TX_MEMPOOL, prev_hash, 
                                                  NODE_ADDRESS, cur_block_height)
                            for i in range(NUM_TX_MEMPOOL):
                                transaction = mempool.pop(0)
                                unmined_block.add_transaction(transaction)
                            # begin the mining process in a separate thread
                            miner_thread = Miner(unmined_block, interrupt_event, finish_event, nonce_q)
                            miner_thread.start()
                            currently_mining = True
                    elif msg_type == OPCODE_BLOCK:
                        # switch this connection to READ_BLOCK mode, in which it
                        # repeatedly reads whenever able, until an entire block
                        # has been read
                        print("Switching this connection to READ_BLOCK mode!")
                        conn_state[conn] = CONN_STATE_READ_BLOCK
                        bytes_remaining[conn] = BLOCK_MSG_LEN
                        conn_block[conn] = bytearray(BLOCK_MSG_LEN)
                    elif msg_type == OPCODE_GETBLOCK:
                        print("Received GET_BLOCK request on connection", conn)
                        request_height = int(conn.recv(32))
                    elif msg_type == OPCODE_GETHASH:
                        print("Received GET_HASH request on connection", conn)
                    elif msg_type == OPCODE_PORTS:
                        print("Received updated ports list")
                        known_ports = update_ports(conn)
                        print("New ports are", known_ports)
                        writers = update_writers(known_ports, writers, port)
                elif conn_state[conn] == CONN_STATE_READ_BLOCK:
                    # continue reading block data over this connection
                    bytes_ind = BLOCK_MSG_LEN - bytes_remaining[conn]
                    remote_bytes = conn.recv(bytes_remaining[conn])
                    num_bytes_read = len(remote_bytes)
                    print("Read", num_bytes_read, "bytes from the current block...")
                    conn_block[conn][bytes_ind:bytes_ind+num_bytes_read] = remote_bytes
                    bytes_remaining[conn] -= num_bytes_read
                    # if we are done reading the block, verify the block and 
                    # reset this connection to its normal state
                    if bytes_remaining[conn] == 0:
                        nonce, prior_hash, block_hash, block_height, block_miner_addr, block_data = parse_block(conn_block[conn])
                        del conn_block[conn]
                        conn_state[conn] = CONN_STATE_READY
                        # signal the miner thread to stop, if it is still active
                        interrupt_event.set()

        # check the progress of our mining
        if finish_event.is_set():
            # mining finished!
            miner_results = nonce_q.get()
            print("Found working nonce", miner_results[0], "with hash", miner_results[1])
            finish_event.clear()
            # broadcast the block to all other nodes
            send_block(conns_to_write, unmined_block, miner_results[0], miner_results[1])
            # add the mined block to our blockchain
            bc.add_block(unmined_block, cur_block_height, miner_results[1])
            cur_block_height += 1
            prev_hash = miner_results[1]
            currently_mining = False
