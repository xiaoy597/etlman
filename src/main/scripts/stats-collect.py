import sys
import os
import time
import Queue
import threading

queue = Queue.Queue()


def process(thread_num, database_name):
    print "Thread %d started." % thread_num
    time.sleep(thread_num * 3)
    while True:
        try:
            table_name = queue.get(block=False)
        except Queue.Empty:
            break

        print "Thread %d got table %s" % (thread_num, table_name)
        print "Thread %d starting stats collection for %s.%s" % (thread_num, database_name, table_name)
        sys.stdout.flush()

        ret = os.system("./submit-stats.sh %s %s" % (database_name, table_name))
        if ret != 0:
            print "Thread %d failed to collect stats for %s.%s" % (thread_num, database_name, table_name)
        else:
            print "Thread %d collecting stats for %s.%s succeeded" % (thread_num, database_name, table_name)

    print "Thread %d exited." % thread_num
    sys.stdout.flush()


def main():
    if len(sys.argv) < 3:
        print "Must specify the database name and the number of parallel tasks."
        exit(1)

    database_name = sys.argv[1]

    num_threads = int(sys.argv[2])
    if num_threads == 0:
        print "Invalid number of parallel tasks of %s" % num_threads
        exit(1)

    threads = []

    for line in sys.stdin:
        line = line.strip("\n ")
        if line == 'TBL_NAME':
            continue
        print line
        queue.put(line)

    for i in range(num_threads):
        t = threading.Thread(target=process, args=(i, database_name))
        threads.append(t)

    print "Stats collection started at: ", time.ctime()
    sys.stdout.flush()

    for i in range(num_threads):
        threads[i].start()

    for i in range(num_threads):
        threads[i].join()

    print "Stats collection finished at: ", time.ctime()

if __name__ == '__main__':
    main()
