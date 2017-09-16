from utils import *

class Block:
    """
    Represents a block of MVBCoin transactions.
    """

    def __init__(self, prev_hash, node_address, height):
        self.prev_hash = prev_hash
        self.node_address = node_address
        self.height = height
        self.transactions = set()
        # when the block is ready to mine we will cache the byte representation
        # so we don't have to repeatedly compute it
        self.byte_repr = None

    def add_transaction(self, transaction):
        self.transactions.add(transaction)
    
    def contains_transaction(self, transaction):
        return transaction in self.transactions

    def set_nonce(self, nonce):
        self.nonce = nonce

    def set_hash(self, block_hash):
        self.block_hash = block_hash

    @property
    def num_transactions(self):
        return len(self.transactions)

    def as_bytearray(self):
        # block data consists of all the transactions in this block, in
        # descending order of amount
        if self.byte_repr is None:
            blockdata = b''.join(sorted(self.transactions, 
                                        key=lambda t: int(t[64:96]), 
                                        reverse=True))
            self.byte_repr = self.prev_hash + int_to_bytes(self.height, 32) + self.node_address + blockdata
        return self.byte_repr

