import hashlib
import socket
import binascii
import select
import threading
import random
import copy
import multiprocessing
from collections import Counter
from queue import Queue
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

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
MAX_NONCE = 2**32 - 1

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
# flag to indicate whether the miner thread is active
currently_mining = False
# controls verbosity setting for all functions in this file
verbose = False

# Process pool for parallel mining (to be initialized based on numcores)
process_pool = None
# Thread pool for concurrently working with shared data
thread_pool = None
# used to signal miner threads when an external block comes in
interrupt_event = multiprocessing.Event()

# drop-in replacement for print which only prints when the verbose flag is set
def print_if_verbose(*args):
    if verbose:
        print(*args)

def mine(block, difficulty, nonce_seed=0):
    print_if_verbose("Miner thread started!")
    bytes_to_hash = bytearray(128 + 128 * block.num_transactions)
    # leave the first 32 bits of bytes_to_hash for the nonce
    bytes_to_hash[32:] = block.as_bytearray()
    while not interrupt_event.is_set():
        # keep looping until we find a nonce that works, OR we get
        # interrupted by an incoming block
        nonce = int_to_bytes(nonce_seed, 32)
        bytes_to_hash[:32] = nonce
        h = hashlib.sha256(bytes_to_hash).digest()
        if h[:difficulty] == b'0' * difficulty:
            # we found a working nonce!
            print_if_verbose("[MINER] Nonce found!")
            # the miner thread is done
            return nonce, h
        nonce_seed = (nonce_seed + 1) % MAX_NONCE
    print_if_verbose("[MINER] Interrupted by incoming block!")
    return None

def hash_matches_blockchain(other_hash):
    """
    Check if the given hash matches the hash of the last block in our current
    blockchain
    """
    if blockchain_height() == -1:
        return True
    return other_hash == blockchain[-1].block_hash

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

def update_writers(port_list, writers, own_ports):
    """
    Given a list of ports, update the collection of write sockets so that there
    is a writer for every port in the list
    """
    for port in port_list:
        if port not in writers and port not in own_ports:
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

def remove_mempool_duplicates(blocks):
    """
    Remove any transactions from the mempool that also show up in blocks, which
    is a list of blocks to check against. This function gets called when one or
    more blocks from another node are accepted into our blockchain.
    """
    global mempool

    print_if_verbose("Scanning mempool for duplicate transactions...")
    new_mempool = []
    for transaction in mempool:
        # check if this transaction is found in any of the new blocks
        tx_is_duplicate = False
        for block in blocks:
            if block.contains_transaction(transaction):
                tx_is_duplicate = True
        if not tx_is_duplicate:
            new_mempool.append(transaction)
    # if we have dropped any duplicate transactions, commit those changes to the
    # mempool
    if len(new_mempool) != len(mempool):
        print("Dropped", len(mempool) - len(new_mempool), "duplicate transactions")
        mempool = new_mempool

def accept_remote_block(remote_block):
    """
    This function is called when a block is received via SEND_BLOCK that is at
    the same height as the block currently being mined. If this happens, we
    must undo the changes made to the UTXO set by the unmined block, validate
    the new block, and accept the new block. Any transactions that were in
    the unmined block and not in the received block will be placed back onto
    the start of the mempool for inclusion in a future block. 
    """
    global mempool

    # this should never be called when there is no block being mined
    assert(unmined_block is not None)
    # first, undo the transactions in the unmined block
    print_if_verbose("Undoing uncommitted UTXO changes...")
    for transaction in unmined_block.transactions:
        sender, receiver, amt, timestamp = parse_transaction(transaction)
        utxo[sender] += amt
        utxo[receiver] -= amt
        # push the undone transaction back into mempool
        mempool.append(transaction)
    # commit all transactions to the UTXO set
    for transaction in remote_block.transactions:
        sender, receiver, amt, timestamp = parse_transaction(transaction)
        utxo[sender] -= amt
        utxo[receiver] += amt
        tx_history.add(transaction)
    # add the remote block to the end of the blockchain
    blockchain.append(remote_block)
    # remove any duplicate transactions from the mempool
    remove_mempool_duplicates([remote_block])

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

def get_blocks(ending_height, initial_hash, block_msg_len, num_tx_in_block, writers):
    """
    Retrieve all blocks starting from ending height until a common ancestor is
    found in our blockchain.
    """
    desired_hash = initial_hash
    current_height = ending_height
    blocks_found = []
    # we will loop until a common ancestor is found
    while True:
        try:
            # if current_height has hit -1 that specifies that we have gotten all
            # the way to the start of the blockchain, in which case we must return
            # immediately as there are no more blocks to look for
            if current_height == -1:
                return list(reversed(blocks_found))
            # if the hash we are looking for is already in our blockchain at the
            # specified height, we are done and can return.
            if current_height <= blockchain_height() and desired_hash == blockchain[current_height].block_hash:
                # we built the list of blocks in reverse order, so return it in
                # the proper order.
                return list(reversed(blocks_found))
            # send a GET_HASH request to all peers
            _, writable_conns, _ = select.select([], writers, [], 5.0)
            msg = OPCODE_GETHASH + int_to_bytes(current_height, 32)
            for conn in writable_conns:
                conn.send(msg)
            # await the reply
            readable_conns, _, _ = select.select(writers, [], [], 5.0)
            for conn in readable_conns:
                other_hash = conn.recv(32)
                if other_hash == desired_hash:
                    # this peer has the block we are looking for!
                    block = get_block(conn, current_height, block_msg_len, num_tx_in_block)
                    blocks_found.append(block)
                    # now we must search for the previous block pointed to by the
                    # newly received block's prev_hash
                    desired_hash = block.prev_hash
                    current_height -= 1
        except TimeoutError:
            return None

def handle_new_block(remote_block, num_tx_in_block, block_msg_len, writers):
    """
    Logic to handle a new block sent from a peer node.
    Implements Kevin Sekniqi's algorithm posted on Piazza.
    """
    global mempool, currently_mining, unmined_block, blockchain

    # If the block we received is one higher than our current blockchain
    # height (not including the unmined block) and consistent with our
    # current blockchain, we should accept it. However, if we are currently
    # mining a block at that height, we need to stop the mining and
    # undo any changes associated with it.
    if remote_block.height == blockchain_height() + 1 and hash_matches_blockchain(remote_block.prev_hash):
        if currently_mining:
            # signal the miner to stop
            interrupt_event.set()
            # drop our unmined block and accept the new one
            accept_remote_block(remote_block)
            currently_mining = False
            unmined_block = None
        else:
            # append the new block to our blockchain and add its transactions
            # to our history
            blockchain.append(remote_block)
            for transaction in remote_block.transactions:
                sender, receiver, amt, timestamp = parse_transaction(transaction)
                utxo[sender] -= amt
                utxo[receiver] += amt
                tx_history.add(transaction)
    elif (remote_block.height > blockchain_height() + 1) or \
         (remote_block.height == blockchain_height() + 1 and not hash_matches_blockchain(remote_block.prev_hash)):
        print_if_verbose("Synchronizing blockchain with global state...")
        # first, stop any mining currently in progress
        if currently_mining:
            interrupt_event.set()
            print_if_verbose("Undoing uncommitted UTXO changes...")
            for transaction in unmined_block.transactions:
                sender, receiver, amt, timestamp = parse_transaction(transaction)
                utxo[sender] += amt
                utxo[receiver] -= amt
                # push this transaction back into the mempool
                mempool.append(transaction)
            currently_mining = False
            unmined_block = None
        # find all blocks up until a common ancestor
        print_if_verbose("Requesting blocks from peers...")
        new_blocks = get_blocks(remote_block.height - 1, remote_block.prev_hash,
                                block_msg_len, num_tx_in_block, writers)
        if new_blocks is None:
            print_if_verbose("No peers responded within 5 seconds, aborting!")
        else:
            new_blocks.append(remote_block)
            # undo all transactions in blocks following the common ancestor
            print("Undoing tranasctions in", blockchain_height() - new_blocks[0].height + 1, "blocks")
            for block in blockchain[new_blocks[0].height:]:
                for transaction in block.transactions:
                    sender, receiver, amt, timestamp = parse_transaction(transaction)
                    utxo[sender] += amt
                    utxo[receiver] -= amt
                    # push this transaction pack into the mempool
                    mempool.append(transaction)
            # drop all blocks after the common ancestor
            blockchain = blockchain[:new_blocks[0].height]
            # then append the newly received blocks
            blockchain += new_blocks
            # commit all transactions in the new blocks into the mempool
            for block in new_blocks:
                sender, receiver, amt, timestamp = parse_transaction(transaction)
                utxo[sender] -= amt
                utxo[receiver] += amt
                tx_history.add(transaction)
            # finally, clean up any duplicate transactions in the mempool
            remove_mempool_duplicates(new_blocks)
            print_if_verbose("Synchronization complete! Blockchain now has height", blockchain_height())
    elif remote_block.height < blockchain_height() + 1:
        # propagate all valid transactions in this block
        for transaction in remote_block.transactions:
            writable_conns, _, _ = select.select([], writers, [])
            sender, receiver, amt, timestamp = parse_transaction(transaction)
            if utxo[sender] >= amt:
                for writer in writable_conns:
                    writer.sendall(b'0' + transaction)

def miner_node(num_workers, ports, num_tx_in_block, difficulty, num_cores, verbose_setting=False):
    # initialize shared values that depend on command line inputs
    global verbose, process_pool
    verbose = verbose_setting
    # one thread is reserved for the main loop
    if num_cores > 1:
        process_pool = ProcessPoolExecutor(num_cores - 1)

    # keep track of futures representing miner threads that are either running
    # or scheduled to run
    running_miners = []

    # initialize the UTXO set
    for i in range(100):
        utxo[hashlib.sha256(bytes(str(i), encoding='ascii')).digest()] = 100000

    # list of known ports we can write to (initially empty)
    known_ports = []
    # connections to other nodes, indexed by their ports
    writers = {}

    # create a socket on each port on which to listen to other nodes
    node_socks = []
    for port in ports:
        node_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_sock.bind(("localhost", port))
        node_sock.listen()
        node_socks.append(node_sock)

    # send port numbers concatenated with this node's address to bootnode
    for port in ports:
        boot_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        boot_sock.connect(("localhost", BOOTSTRAP_PORT))
        msg = int_to_bytes(port) + NODE_ADDRESS
        boot_sock.sendall(msg)

    # list of connections that could receive new messages. This includes
    # our own sockets (which could receive an incoming connection) and
    # any currently active external connections
    read_conns = copy.copy(node_socks)

    global unmined_block, mempool, currently_mining

    # set the length of a block message based on the number of transactions
    # allowed in a block
    block_msg_len = 160 + 128 * num_tx_in_block

    # listen for messages from other nodes
    while True:
        conns_to_read, conns_to_write, _ = select.select(read_conns, list(writers.values()), [])
        for conn in conns_to_read:
            if conn in node_socks:
                # if a server socket is available, this means there is a new
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
                elif msg_type == OPCODE_BLOCK:
                    # read a block over the external connection
                    print_if_verbose("Received block on connection", conn)
                    remote_block_data = read_block(conn, block_msg_len)
                    remote_block = parse_block(remote_block_data, num_tx_in_block)
                    # ignore malformed blocks
                    if remote_block is not None:
                        handle_new_block(remote_block, num_tx_in_block, block_msg_len, 
                                         list(writers.values()))
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
                    writers = update_writers(known_ports, writers, ports)
            # if the mempool now contains at least 50000 transactions,
            # AND the miner thread is not busy, add them to a block and mine
            if len(mempool) >= num_tx_in_block and not currently_mining:
                # create a new block for mining
                print_if_verbose("Mining new block at height", blockchain_height() + 1)
                unmined_block = Block(latest_hash(), NODE_ADDRESS, blockchain_height() + 1)
                # grab the first num_tx_in_block transactions from the mempool,
                # add them to the block and remove them from the mempool
                block_tx = mempool[:num_tx_in_block]
                mempool = mempool[num_tx_in_block:]
                for transaction in block_tx:
                    unmined_block.add_transaction(transaction)
                # begin the mining process
                if process_pool is not None:
                    interrupt_event.clear()
                    for i in range(num_cores - 1):
                        nonce_seed = random.randint(0, MAX_NONCE)
                        future = process_pool.submit(mine, unmined_block, difficulty, nonce_seed)
                        running_miners.append(future)
                    currently_mining = True
        # check the progress of our mining
        if any(f.done() for f in running_miners):
            all_interrupted = True
            for future in running_miners:
                if future.done() and future.result() is not None:
                    # the result of this future is a valid nonce
                    all_interrupted = False
                    # stop all the other miners
                    interrupt_event.set()
                    # retrieve the nonce and accompanying hash
                    miner_results = future.result()
                    print_if_verbose("Found working nonce", miner_results[0], "with hash", miner_results[1])
                    unmined_block.set_nonce(miner_results[0])
                    unmined_block.set_hash(miner_results[1])
                    # broadcast the block to all other nodes
                    process_pool.submit(send_block, conns_to_write, unmined_block, 
                                        miner_results[0], miner_results[1])
                    # add the mined block to our blockchain
                    blockchain.append(unmined_block)
                    currently_mining = False
                    # discard the other futures since we don't need them anymore
                    running_miners = []
                elif not future.done():
                    all_interrupted = False
            # if the miners are all done but none of them returned anything, that
            # means they were interrupted by an incoming block. In that case,
            # discard the futures as we don't need to keep track of them anymore
            if all_interrupted:
                running_miners = []
