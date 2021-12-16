#
# Convert YCSB text trace (reported by ycsb load/run basic on a workload)
# to trace format suitable as input to ycsbcli.
#

import sys


def main(fname_in, fname_out):
    with open(fname_in, 'r') as fi, open(fname_out, 'w') as fo:
        for line in fi.readlines():
            segs = line.strip().split()
            if segs[0] == "READ" or segs[0] == "INSERT" or segs[0] == "UPDATE":
                # opcode key
                fo.write(f"{segs[0]} {segs[2]}\n")
            elif segs[0] == "SCAN":
                # scan key length
                fo.write(f"{segs[0]} {segs[2]} {segs[3]}\n")


if __name__ == "__main__":
    assert len(sys.argv) == 3
    fname_in = sys.argv[1]
    fname_out = sys.argv[2]

    main(fname_in, fname_out)
