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
# The block currently being mined, if any
unmined_block = None
# the hash of the last block we mined or received
prev_hash = hashlib.sha256(b'0').digest()
# the blockchain itself, implemented as a list of Blocks
blockchain = []

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

def blockchain_height():
    """
    Returns the height of the currently accepted blockchain.
    -1 means an empty blockchain
    """
    return len(blockchain) - 1

def transaction_is_duplicate(transaction):
    """
    Check whether the given transaction already exists in the mempool
    or in the unmined block
    """
    return (transaction in mempool) or \
           (unmined_block is not None and unmined_block.contains_transaction(transaction))

def handle_transaction(ext_conn, writers):
    """
    Handle a new transaction being sent over the external connection ext_conn.
    """
    transaction = ext_conn.recv(128)
    print("Received transaction")
    # don't repeat process out this transaction if we have already seen it
    if transaction_is_duplicate(transaction):
        print("Transaction is duplicate, skipping...")
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
    # construct a Block object using the received data
    new_block = Block(prior_hash, block_miner_addr, block_height)
    for block_data_ind in range(0, 128 * NUM_TX_MEMPOOL, 128):
        new_block.add_transaction(bytes(block_data[block_data_ind:block_data_ind+128]))
    new_block.set_nonce(nonce)
    new_block.set_hash(block_hash)
    return new_block

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

def read_block(ext_conn):
    # buffer to store the block being read
    buf = bytearray(BLOCK_MSG_LEN)
    bytes_to_read = BLOCK_MSG_LEN
    while bytes_to_read > 0:
        readable_conns, _, _ = select.select([ext_conn], [], [])
        if len(readable_conns) == 1:
            received = readable_conns[0].recv(bytes_to_read)
            bytes_read = len(received)
            buf_ind = BLOCK_MSG_LEN - bytes_to_read
            buf[buf_ind:buf_ind+bytes_read] = received
            bytes_to_read -= bytes_read
    return buf

def drop_unmined_block(remote_block):
    """
    This function is called when a block is received via SEND_BLOCK that is at
    the same height as the block currently being mined. If this happens, we
    must undo the changes made to the UTXO set by the unmined block, validate
    the new block, and accept the new block. Any transactions that were in
    the unmined block and not in the received block will be placed back onto
    the start of the mempool for inclusion in a future block. Returns whether
    or not we chose to accept the remote block.
    """
    global mempool

    # this should never be called when there is no block being mined
    assert(unmined_block is not None)
    # validate the new block by checking its prior hash against the unmined
    # block's. If they do not match, we have a fork with tied lengths, so to
    # keep things simple we temporarily keep our own fork.
    if remote_block.prev_hash != unmined_block.prev_hash:
        return False
    # first, undo the transactions in the unmined block
    print("Undoing uncommitted UTXO changes...")
    for transaction in unmined_block.transactions:
        sender, receiver, amt, timestamp = parse_transaction(transaction)
        utxo[sender] += amt
        utxo[receiver] -= amt
    # commit all transactions to the UTXO set
    for transaction in remote_block.transactions:
        sender, receiver, amt, timestamp = parse_transaction(transaction)
        utxo[sender] -= amt
        utxo[receiver] += amt
    blockchain.append(remote_block)
    # any transactions that were in the unmined block but not in the new
    # block must be placed back at the head of the mempool
    unhandled_tx = []
    for transaction in unmined_block.transactions:
        if not remote_block.contains_transaction(transaction):
            unhandled_tx.append(transaction)
        if len(unhandled_tx) > 0:
            print("Placing", len(unhandled_tx), "unhandled transactions back into mempool")
            mempool = unhandled_tx + mempool
    return True

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

    global prev_hash, cur_block_height, unmined_block

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
            else:
                # this must be a new message from a previously connected node
                # first, read the opcode so we know how much data to chew
                msg_type = conn.recv(1)
                # separate handlers for each type of message
                if msg_type == OPCODE_TRANSACTION:
                    handle_transaction(conn, conns_to_write)
                    # if the mempool now contains at least 50000 transactions,
                    # AND the miner thread is not busy, add them to a block and mine
                    if len(mempool) >= NUM_TX_MEMPOOL and not currently_mining:
                        # create a new block for mining
                        print("Mining new block at height", blockchain_height() + 1)
                        unmined_block = Block(prev_hash, NODE_ADDRESS, blockchain_height() + 1)
                        for i in range(NUM_TX_MEMPOOL):
                            transaction = mempool.pop(0)
                            unmined_block.add_transaction(transaction)
                        # begin the mining process in a separate thread
                        miner_thread = Miner(unmined_block, interrupt_event, finish_event, nonce_q)
                        miner_thread.start()
                        currently_mining = True
                elif msg_type == OPCODE_BLOCK:
                    # read a block over the external connection
                    remote_block_data = read_block(conn)
                    remote_block = parse_block(remote_block_data)
                    # if the miner is still mining a block of the same height as the one
                    # we just received, we need to kill it and accept the received block
                    if currently_mining and remote_block.height == blockchain_height() + 1:
                        interrupt_event.set()
                        remote_block_accepted = drop_unmined_block(remote_block)
                        if not remote_block_accepted:
                            miner_thread = Miner(unmined_block, interrupt_event, finish_event, nonce_q)
                            miner_thread.start()
                            currently_mining = True
                        else:
                            currently_mining = False
                elif msg_type == OPCODE_GETBLOCK:
                    print("Received GET_BLOCK request on connection", conn)
                    request_height = int(conn.recv(32))
                    if request_height <= blockchain_height():
                        send_block([conn], blockchain[request_height],
                                   blockchain[request_height].nonce,
                                   blockchain[request_height].block_hash)
                elif msg_type == OPCODE_GETHASH:
                    print("Received GET_HASH request on connection", conn)
                    request_height = int(conn.recv(32))
                    if request_height <= blockchain_height():
                        conn.send(blockchain[request_height].block_hash)
                elif msg_type == OPCODE_PORTS:
                    print("Received updated ports list")
                    known_ports = update_ports(conn)
                    print("New ports are", known_ports)
                    writers = update_writers(known_ports, writers, port)

        # check the progress of our mining
        if finish_event.is_set():
            # mining finished!
            miner_results = nonce_q.get()
            print("Found working nonce", miner_results[0], "with hash", miner_results[1])
            finish_event.clear()
            unmined_block.set_nonce(miner_results[0])
            unmined_block.set_hash(miner_results[1])
            # broadcast the block to all other nodes
            send_block(conns_to_write, unmined_block, miner_results[0], miner_results[1])
            # add the mined block to our blockchain
            blockchain.append(unmined_block)
            cur_block_height += 1
            prev_hash = miner_results[1]
            currently_mining = False
            unmined_block = None
