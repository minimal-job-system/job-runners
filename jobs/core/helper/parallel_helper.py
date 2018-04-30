class ParallelHelperClass(object):
    """
    Helper class with functions for parallelization.
    """
    #TODO: remove unused class

    def split_seq(self, seq, m):
        """
        Split a sequence `seq` into m quasi equal partitions
        (decreasing order).
        @see also: https://en.wikipedia.org/wiki/Modulo_operation

        :param a: divident, start index of the k-th partition
        :param b: end indx of the k-th partition
        :param n: divisor
        :param q: quotient
        :param r: remainder
        :param n: length of the input sequence
        :param m: number of partitions
        :return: partitioned sequence
        """
        n, b = len(seq), 0
        for k in range(m):
            q, r = divmod(n-k, m)
            a, b = b, b + q + (r != 0)
            yield seq[a:b]
