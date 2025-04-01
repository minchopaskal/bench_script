import redis
import sys
import subprocess


prefix = ''
if len(sys.argv) > 1:
    prefix = sys.argv[1]

r = redis.StrictRedis(host='localhost', port=6379, charset='ascii', decode_responses=True)

# sizes = [1024, 12500, 1250000]
# keycnt = [16, 330000]
sizes = [10, 20]
keycnt = [10, 20]
for sz in sizes:
    for cnt in keycnt:
        r.flushall();
        memtier_cmd="memtier_benchmark --command='set __key__ __data__' -c1 -t1 -n{} -d{} --randomize -R".format(cnt, sz)
        subprocess.run(memtier_cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        keys = r.keys('memtier-*')

        # ops = ['and', 'or', 'xor', 'not', 'diff', 'diff1', 'andor', 'one']
        ops = ['and', 'or']
        for op in ops:
            memtier_cmd='bitop {} dst:{} '.format(op, op)
            for key in keys:
                memtier_cmd += key
                memtier_cmd += ' '
            curr_prefix = "{}-{}-{}-{}".format(prefix, op, sz, cnt)
            memtier_cmd = "memtier_benchmark --command='{}' -n10000 --hdr-file-prefix={} --hide-histogram | tail -n2 | head -n1".format(memtier_cmd, curr_prefix)
            print("RUNNING: {}".format(memtier_cmd))
            with open("{}_log.txt".format(curr_prefix), 'w') as f:
                process = subprocess.run(memtier_cmd, shell=True, stdout=f, stderr=subprocess.DEVNULL)

