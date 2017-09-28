import hashlib
import socket
import binascii
import select
import threading
import random
import time
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

# The UTXO set stores all known addresses that can spend money
utxo = Counter()
# The mempool stores all transactions that have not yet been committed
mempool = []
# The block currently being mined, if any
unmined_block = None
# the blockchain itself, implemented as a list of Blocks
blockchain = []
# the set of all transactions we have ever received (either directly or as part
# of an accepted block from another node)
tx_history = set()
# controls verbosity setting for all functions in this file
verbose = False

# drop-in replacement for print which only prints when the verbose flag is set
def print_if_verbose(*args):
    if verbose:
        print(*args)

class Miner(threading.Thread):
    """
    Thread that performs block mining in the background, allowing the
    main thread to continue listening for messages
    """
    def __init__(self, block, difficulty, interrupt_event, finish_event, result_q, 
                 group=None, target=None, name=None, args=(), kwargs={}, daemon=None):
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)
        self.block = block
        self.difficulty = difficulty
        self.interrupt_event = interrupt_event
        self.finish_event = finish_event
        self.result_q = result_q

    def run(self):
        print_if_verbose("Miner thread started!")
        nonce_seed = 0
        bytes_to_hash = bytearray(128 + 128 * self.block.num_transactions)
        # leave the first 32 bits of bytes_to_hash for the nonce
        bytes_to_hash[32:] = self.block.as_bytearray()
        while not self.interrupt_event.is_set():
            # keep looping until we find a nonce that works, OR we get
            # interrupted by an incoming block
            nonce = int_to_bytes(nonce_seed, 32)
            bytes_to_hash[:32] = nonce
            h = hashlib.sha256(bytes_to_hash).digest()
            if h[:self.difficulty] == b'0' * self.difficulty:
                # we found a working nonce!
                print_if_verbose("[MINER] Nonce found!")
                self.result_q.put((nonce, h))
                self.finish_event.set()
                # the miner thread is done
                return
            nonce_seed += 1
        print_if_verbose("[MINER] Interrupted by incoming block!")
        self.interrupt_event.clear()

def parse_ports(msg):
    # extract the list of ports from the reply
    ports = []
    for i in range(0, len(msg), 6):
        ports.append(int(msg[i:i+6]))
    print_if_verbose(ports)
    return ports

def update_ports(ext_conn):
    """
    Handle a port list update being sent over the external connection ext_conn
    """
    num_conns = int(ext_conn.recv(6))
    return parse_ports(ext_conn.recv(num_conns * 6))

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

def latest_hash():
    """
    Get the hash of the block at the end of the current blockchain.
    If the blockchain is empty, returns sha256('0') in accordance with spec
    """
    if len(blockchain) == 0:
        return hashlib.sha256(b'0').digest()
    else:
        return blockchain[-1].block_hash

def handle_transaction(ext_conn, writers):
    """
    Handle a new transaction being sent over the external connection ext_conn.
    """
    transaction = ext_conn.recv(128)
    print_if_verbose("Received transaction")
    # don't repeat process out this transaction if we have already seen it
    if transaction in tx_history or transaction in mempool:
        print_if_verbose("Transaction is duplicate, skipping...")
        return
    sender, receiver, amt, timestamp = parse_transaction(transaction)
    print_if_verbose("From:", binascii.hexlify(sender))
    print_if_verbose("To:", binascii.hexlify(receiver))
    print_if_verbose("Amount:", amt)
    print_if_verbose("Timestamp:", timestamp)
    # check if the transaction is valid (sender must have enough coins
    # according to the UTXO set)
    if amt <= utxo[sender]:
        print_if_verbose("Sender has enough MVBCoin, transaction is valid")
        # propagate the transaction
        for writer in writers:
            writer.sendall(b'0' + transaction)
        # queue up the transaction in the mempool
        mempool.append(transaction)
        # record that this transaction has been seen before
        tx_history.add(transaction)
        # update the balances of the sender and receiver
        utxo[sender] -= amt
        utxo[receiver] += amt
    else:
        print_if_verbose("Transaction invalid; sender has %d MVBCoin but tried to send %d. Skipping..." % (utxo[sender], amt))

def parse_block(block_bytes, num_tx):
    """
    Parse a block sent over a socket as raw bytes
    """
    try:
        nonce = block_bytes[0:32]
        prior_hash = block_bytes[32:64]
        block_hash = block_bytes[64:96]
        block_height = int(block_bytes[96:128])
        block_miner_addr = block_bytes[128:160]
        block_data = block_bytes[160:]
    except BaseException as e:
        print_if_verbose("Invalid block encountered; aborting! (Error:", e, ")")
        return None
    print_if_verbose("Finished receiving block from node at address", block_miner_addr, "with height", block_height)
    # construct a Block object using the received data
    new_block = Block(prior_hash, block_miner_addr, block_height)
    for block_data_ind in range(0, 128 * num_tx, 128):
        new_block.add_transaction(bytes(block_data[block_data_ind:block_data_ind+128]))
    new_block.set_nonce(nonce)
    new_block.set_hash(block_hash)
    return new_block

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

def send_block(writers, block, nonce, block_hash, include_opcode=True):
    """
    Send a block that we have mined out to all other nodes
    """
    # get the byte representation of the block
    block_bytes = block.as_bytearray()
    # create a buffer to store our message
    msg = bytearray(161 + 128 * block.num_transactions)
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
    # if not including an opcode (special case for reply to GET_BLOCK), strip
    # off the first byte
    if not include_opcode:
        msg = msg[1:]
    print_if_verbose("Sending message with length", len(msg))
    for writer in writers:
        write_block(msg, writer)

def read_block(ext_conn, block_msg_len):
    # buffer to store the block being read
    buf = bytearray(block_msg_len)
    bytes_to_read = block_msg_len
    while bytes_to_read > 0:
        readable_conns, _, _ = select.select([ext_conn], [], [])
        if len(readable_conns) == 1:
            received = readable_conns[0].recv(bytes_to_read)
            bytes_read = len(received)
            buf_ind = block_msg_len - bytes_to_read
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
    print_if_verbose("Undoing uncommitted UTXO changes...")
    for transaction in unmined_block.transactions:
        sender, receiver, amt, timestamp = parse_transaction(transaction)
        utxo[sender] += amt
        utxo[receiver] -= amt
    # commit all transactions to the UTXO set
    for transaction in remote_block.transactions:
        sender, receiver, amt, timestamp = parse_transaction(transaction)
        utxo[sender] -= amt
        utxo[receiver] += amt
        tx_history.add(transaction)
    blockchain.append(remote_block)
    # any transactions that were in the unmined block but not in the new
    # block must be placed back at the head of the mempool
    unhandled_tx = []
    for transaction in unmined_block.transactions:
        if not remote_block.contains_transaction(transaction):
            unhandled_tx.append(transaction)
    if len(unhandled_tx) > 0:
        print_if_verbose("Placing", len(unhandled_tx), "unhandled transactions back into mempool")
        mempool = unhandled_tx + mempool
    return True

def get_block(ext_conn, height, block_msg_len, num_tx_in_block):
    """
    Send a GET_BLOCK request for the block at height over ext_conn.
    Return the block sent back over the connection
    """
    msg = OPCODE_GETBLOCK + int_to_bytes(height, 32)
    ext_conn.send(msg)
    # read the reply and return it
    block_data = read_block(ext_conn, block_msg_len)
    return parse_block(block_data, num_tx_in_block)

def sync_blockchain_helper(writers, height, desired_hash, block_msg_len, num_tx_in_block):
    """
    Recursive helper function for synchronize_blockchain. Does the actual work
    of retrieving blocks over a remote connection
    """
    # Base case #1: we have hit the end of the blockchain and should abort
    if height == -1:
        return
    # Base case #2: our blockchain already contains the block with the desired
    # hash in the given position, so we are done
    if blockchain[height] is not None and blockchain[height].block_hash == desired_hash:
        return
    # Recursive case: search the network for a node with the desired hash, insert
    # the found node, and recurse on its previous hash
    _, writable_conns, _ = select.select([], writers, [])
    msg = OPCODE_GETHASH + int_to_bytes(height, 32)
    for conn in writable_conns:
        conn.send(msg) # send the GET_HASH message to all connections
    # await the reply
    readable_conns, _, _ = select.select(writers, [], [])
    for conn in readable_conns:
        other_hash = conn.recv(32)
        if other_hash == desired_hash:
            # we found the block! now we need to add it to the blockchain and recurse...
            block = get_block(conn, height, block_msg_len, num_tx_in_block)
            # adding the block to the blockchain may overwrite an existing block,
            # in which case we need to undo its transactions
            if blockchain[height] is not None:
                print_if_verbose("Undoing transactions in discarded block at height", height)
                for transaction in blockchain[height].transactions:
                    sender, receiver, amt, timestamp = parse_transaction(transaction)
                    utxo[sender] += amt
                    utxo[receiver] -= amt
            # insert the block into the blockchain
            blockchain[height] = block
            # commit the transactions in the newly added block to the UTXO set
            for transaction in block.transactions:
                sender, receiver, amt, timestamp = parse_transaction(transaction)
                utxo[sender] -= amt
                utxo[receiver] += amt
                tx_history.add(transaction)
            sync_blockchain_helper(writers, height - 1, block.prev_hash, block_msg_len, num_tx_in_block)
            return

def synchronize_blockchain(writers, ending_block, block_msg_len, num_tx_in_block):
    """
    Synchronize the state of our blockchain with the global state.
    After synchronization, we will have a blockchain ending in the block
    ending_block.
    Algorithm based on Piazza comment by Jake Gardner
    """
    print_if_verbose("Synchronizing blockchain with global state...")
    # extend the list representing our blockchain so that it's height matches
    # that of the global one. Any slots between our current height and that of
    # ending_block will be temporarily filled with Nones
    for i in range(blockchain_height() + 1, ending_block.height + 1):
        blockchain.append(None)
    # insert ending_block into the last slot in the extended blockchain
    blockchain[ending_block.height] = ending_block
    # recursively fill in the gaps left when the blockchain was extended
    sync_blockchain_helper(writers, ending_block.height - 1, ending_block.prev_hash, 
                           block_msg_len, num_tx_in_block)
    print_if_verbose("Synchronization complete! Blockchain now has height", blockchain_height())

def miner_node(num_workers, port, num_tx_in_block, difficulty, verbose_setting=False):
    # set the verbosity flag based on input parameter
    global verbose 
    verbose = verbose_setting

    # initialize the UTXO set
    for i in range(100):
        utxo[hashlib.sha256(bytes(str(i), encoding='ascii')).digest()] = 100000

    # list of known ports we can write to (initially empty)
    known_ports = []
    # connections to other nodes, indexed by their ports
    writers = {}

    # create a socket on which to listen to other nodes
    node_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_sock.bind(("localhost", port))
    node_sock.listen(10)

    # send a message to the bootnode informing that we are ready,
    # and get back the list of known ports
    boot_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    boot_sock.connect(("localhost", BOOTSTRAP_PORT))
    # send port number concatenated with this node's address
    msg = int_to_bytes(port) + NODE_ADDRESS
    boot_sock.sendall(msg)

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

    global unmined_block, mempool 

    # set the length of a block message based on the number of transactions
    # allowed in a block
    block_msg_len = 160 + 128 * num_tx_in_block

    # flag to indicate whether the miner thread is active
    currently_mining = False

    start_time = None

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
                    if start_time is None:
                        start_time = time.time()
                    handle_transaction(conn, conns_to_write)
                elif msg_type == OPCODE_BLOCK:
                    # read a block over the external connection
                    print_if_verbose("Received block on connection", conn)
                    remote_block_data = read_block(conn, block_msg_len)
                    remote_block = parse_block(remote_block_data, num_tx_in_block)
                    # ignore malformed blocks
                    if remote_block is None:
                        continue
                    # if the miner is still mining a block of the same height as the one
                    # we just received, we need to kill it and accept the received block
                    if currently_mining and remote_block.height == blockchain_height() + 1:
                        interrupt_event.set()
                        remote_block_accepted = drop_unmined_block(remote_block)
                        if not remote_block_accepted:
                            miner_thread = Miner(unmined_block, difficulty, interrupt_event, finish_event, nonce_q)
                            miner_thread.start()
                            currently_mining = True
                        else:
                            currently_mining = False
                            unmined_block = None
                    # Since the policy is to always mine the longest block, we will
                    # not maintain forks that are shorter than our current blockchain
                    # height. This allows us to save a lot of work, as we never end
                    # up syncing with forks that end up dying off anyway. We will
                    # only fork when the other block is at a height greater than our
                    # current blockchain, and in that scenario we immediately switch
                    # to that branch and discard our current one; we can always
                    # switch back with little cost if it ends up being the wrong branch.
                    # This algorithm was based on discussion with Matt Burke.
                    if remote_block.height > blockchain_height():
                        # first, stop any mining currently in progress
                        unhandled_tx = []
                        if currently_mining:
                            interrupt_event.set()
                            print_if_verbose("Undoing uncommitted UTXO changes...")
                            for transaction in unmined_block.transactions:
                                sender, receiver, amt, timestamp = parse_transaction(transaction)
                                utxo[sender] += amt
                                utxo[receiver] -= amt    
                                unhandled_tx.append(transaction)
                                # temporarily remove this transaction from history
                                tx_history.discard(transaction)
                        # run the recursive synchronization algorithm
                        synchronize_blockchain(list(writers.values()), remote_block, block_msg_len, num_tx_in_block)
                        # flush any unhandled transactions back into the mempool
                        if currently_mining:
                            true_unhandled_tx = []
                            # do not place transactions that were already handled
                            # in the merged chain back into the mempool
                            for transaction in unhandled_tx:
                                if transaction not in tx_history:
                                    true_unhandled_tx.append(transaction)
                            if len(true_unhandled_tx) > 0:
                                print_if_verbose("Placing", len(true_unhandled_tx), "unhandled transactions back into mempool")
                                mempool = true_unhandled_tx + mempool
                            currently_mining = False
                            unmined_block = None
                elif msg_type == OPCODE_GETBLOCK:
                    print_if_verbose("Received GET_BLOCK request on connection", conn)
                    request_height = int(conn.recv(32))
                    # only reply if our blockchain is long enough
                    if request_height <= blockchain_height():
                        send_block([conn], blockchain[request_height],
                                   blockchain[request_height].nonce,
                                   blockchain[request_height].block_hash,
                                   include_opcode=False)
                elif msg_type == OPCODE_GETHASH:
                    print_if_verbose("Received GET_HASH request on connection", conn)
                    request_height = int(conn.recv(32))
                    # only reply if our blockchain is long enough
                    if request_height <= blockchain_height():
                        conn.send(blockchain[request_height].block_hash)
                elif msg_type == OPCODE_PORTS:
                    print_if_verbose("Received updated ports list")
                    known_ports = update_ports(conn)
                    print_if_verbose("New ports are", known_ports)
                    writers = update_writers(known_ports, writers, port)
            # if the mempool now contains at least 50000 transactions,
            # AND the miner thread is not busy, add them to a block and mine
            if len(mempool) >= num_tx_in_block and not currently_mining:
                # create a new block for mining
                print_if_verbose("Mining new block at height", blockchain_height() + 1)
                unmined_block = Block(latest_hash(), NODE_ADDRESS, blockchain_height() + 1)
                for i in range(num_tx_in_block):
                    transaction = mempool.pop(0)
                    unmined_block.add_transaction(transaction)
                # begin the mining process in a separate thread
                miner_thread = Miner(unmined_block, difficulty, interrupt_event, finish_event, nonce_q)
                miner_thread.start()
                currently_mining = True
        # check the progress of our mining
        if finish_event.is_set():
            # mining finished!
            miner_results = nonce_q.get()
            print_if_verbose("Found working nonce", miner_results[0], "with hash", miner_results[1])
            finish_event.clear()
            unmined_block.set_nonce(miner_results[0])
            unmined_block.set_hash(miner_results[1])
            # broadcast the block to all other nodes
            send_block(conns_to_write, unmined_block, miner_results[0], miner_results[1])
            # add the mined block to our blockchain
            blockchain.append(unmined_block)
            currently_mining = False
            unmined_block = None
        if blockchain_height() == 9:
            end_time = time.time()
            print("Time elapsed: %.6f s" % (end_time - start_time))
            return
