import redis
import sys
import subprocess

prefix = ''
if len(sys.argv) > 1:
    prefix = sys.argv[1]

r = redis.StrictRedis(host='localhost', port=6379, charset='ascii', decode_responses=True)

sizes = [32, 1000, 50000, 1000000, 10000000]
keycnt = [2, 5, 16, 64]
times = {32: 2, 1000: 2, 50000: 3, 100000: 5, 1000000: 10, 10000000: 10}
for sz in sizes:
    for cnt in keycnt:
        r.flushall();
        memtier_cmd="memtier_benchmark --command='set __key__ __data__' -c1 -t1 -n{} -d{} --randomize -R".format(cnt, sz)
        subprocess.run(memtier_cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        keys = r.keys('memtier-*')
        assert(len(keys) == cnt)

        ops = ['and', 'not', 'diff', 'one']
        for op in ops:
            memtier_cmd='bitop {} dst:{} '.format(op, op)
            for key in keys:
                memtier_cmd += key
                memtier_cmd += ' '
            curr_prefix = "{}-{}-{}-{}".format(prefix, op, sz, cnt)
            memtier_cmd = "memtier_benchmark --test-time={} --command='{}' --hdr-file-prefix={} --hide-histogram | tail -n2 | head -n1".format(times[sz]*60, memtier_cmd, curr_prefix)
            print("RUNNING: {}".format(memtier_cmd))
            with open("{}_log.txt".format(curr_prefix), 'w') as f:
                process = subprocess.run(memtier_cmd, shell=True, stdout=f, stderr=subprocess.DEVNULL)

