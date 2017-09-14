def zero_pad(s, length):
    """
    Pad the input string s with leading 0's to get it to the specified length
    """
    num_zeros = length - len(s)
    if num_zeros < 0:
        raise ValueError("%s is longer than the target length %d!" % (s, length))
    if num_zeros == 0:
        return s
    return '0' * num_zeros + s

def int_to_bytes(i, n_bytes=6):
    """
    Given an integer i, return a bytearray representation of it;
    i.e. 42 -> b'42'
    """
    return bytes(zero_pad(str(i), n_bytes), encoding="ascii")
