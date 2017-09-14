import random

class Blockchain:
    """
    Class encapsulating the blockchain, including conflict resolution logic
    """

    def __init__(self):
        self.blocks = []
        self.head = None

    def add_block(self, block, height, block_hash):
        if height >= len(self.blocks):
            # add on to the end of the blockchain
            self.blocks.append({})
            self.blocks[height][block_hash] = block
        else:
            # we have a fork!
            self.blocks[height][block_hash] = block
        # if the new block added on to a chain longer than our current one, we
        # should move over to that chain
        if self.head is None or self.head.height < height:
            self.head = random.choice(list(self.blocks[height].values()))

    @property
    def height(self):
        if self.head is None:
            return -1
        else:
            return self.head.height
