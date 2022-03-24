# Import used modules
import pandas as pd, threading, queue, time, csv, re

# initialize terminator to force consumer to stop get from Queue
TERMINATOR = object()
# initialize Queue where Producer and Consumer will communicate
Q = queue.Queue(10)
# Producer Thread to put chuncks into Queue
class Producer(threading.Thread):
    def run(self):
        for chunk in pd.read_csv("G:\College Study\CS Task\Tasks code\Milestone 2\draft\Hussien1.csv",low_memory=False, chunksize=10000,header=None):
            Q.put(chunk)
        Q.put(TERMINATOR)


# Consumer Thread to get chuncks from Queue
class Consumer(threading.Thread):

    def run(self):
        badWords = pd.read_csv("G:\College Study\CS Task\Tasks code\Milestone 2\BadWords.csv",header=None).values.tolist()
        bad_words_regex = re.compile('|'.join(re.escape(x[0]) for x in badWords))
        # define two csv files and their writers
        dirty_records = open("G:\College Study\CS Task\Tasks code\Milestone 2\Dirty_Records.csv", 'w')
        dirty_records_writer = csv.writer(dirty_records)#Unhealthy_Records
        clean_records = open("G:\College Study\CS Task\Tasks code\Milestone 2\Clean_Records.csv", 'w')
        clean_records_writer = csv.writer(clean_records)

        while True:
            selected_chunk = Q.get()
            chunk_time=time.time()
            if selected_chunk is TERMINATOR:
                break
            for index, record in selected_chunk.iterrows():
                # if it is unhealth:
                if re.search(bad_words_regex, str(record[0])) != None or re.match(bad_words_regex, str(record[2])) != None or re.match(bad_words_regex, str(record[4])) != None:
                    dirty_records_writer.writerow(record)
                # else it's health:
                else:
                    clean_records_writer.writerow(record)

            print('chunck is finished within: ',time.time()-chunk_time,' seconds')
# Main thread which run two other threads
a = time.time()
pth = Producer()
pth.start()
cth = Consumer()
cth.start()
cth.join()
print('all Program finished within ',time.time()-a,' seconds')
