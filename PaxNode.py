
__author__ = 'bekzat'
import pika
import json
import threading
import time
from random import randint
import inspect

def whoami():
    return inspect.stack()[1][3]

parameters = pika.ConnectionParameters(
        'localhost', #'192.168.0.103',
        5672,
        '/',
        pika.PlainCredentials('guest','guest'))

heartBeatDelay = 2
messageDelay = 2
N = 5

class PaxNode(object):
    def __init__(self, node_id):
        self.node_id = node_id # uniq id of node in range 1..5


        #TODO: HERE IS HOW MANY QUESTION IN TO DECIDE FOR PARLAMENT
        self.countTotalDecrees = 100

        self.wasLeader = False
        self.ballotLeader = 0 # id of current ballot leader
        self.ballotParticipants = [] # temporary for the last ballot
        self.ballotDecreeConfirmers = [] # temporary for the last ballot

        self.lastTried = self.getLastTried() # last tried ballot, parameter of leader
        self.maxBallot = self.getMaxBallot()

        self.lastAcceptedDecree = self.getLastAcceptedDecree()
        self.decreesList = self.getDecreesList()# inside dict["prev_ballot":"x","prev_vote":"y"]

        print "node restored with: "
        print "last_tried = " + str(self.lastTried)
        print "max_ballot = " + str(self.maxBallot)
        print "last_accepted_decree = " + str(self.lastAcceptedDecree)
        print "decrees_list = " + str(self.decreesList)


        self.lastTimeHeartBeat = 0
        self.lastTimeCurRound = 0
        self.lastTimeDecreeVotedCurRound = 0

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue='next_ballot' + str(node_id))
        channel.queue_declare(queue='last_vote' + str(node_id))
        channel.queue_declare(queue='begin_ballot' + str(node_id))
        channel.queue_declare(queue='voted' + str(node_id))
        # channel.queue_declare(queue='success' + str(node_id))
        channel.queue_declare(queue='heart_beat' + str(node_id))

        channel.basic_consume(self.next_ballot_callback, queue='next_ballot' + str(node_id), no_ack=True)
        channel.basic_consume(self.last_vote_callback, queue='last_vote' + str(node_id), no_ack=True)
        channel.basic_consume(self.begin_ballot_callback, queue='begin_ballot' + str(node_id), no_ack=True)
        channel.basic_consume(self.voted_callback, queue='voted' + str(node_id), no_ack=True)
        channel.basic_consume(self.heart_beat_callback, queue='heart_beat' + str(node_id), no_ack=True)

        print "new thread"
        self.heartBeatThread = threading.Thread(target=self.heart_beat_pinger)
        self.heartBeatThread.start()

        print "listening"
        channel.start_consuming()

# READ VARIABLES FROM DATASOURCE - LOCAL FILE SYSTEM
    def getLastTried(self):
        fin  = open(str(self.node_id)+"_last_tried.txt"); x = map(int, fin.readline().split()); fin.close(); return x[0];
    def getMaxBallot(self):
        fin  = open(str(self.node_id)+"_max_ballot.txt"); x = map(int, fin.readline().split()); fin.close();return x[0];

    def getLastAcceptedDecree(self):
        fin  = open(str(self.node_id)+"_last_accepted_vote.txt");  x = map(int, fin.readline().split()); fin.close(); return x[0];
    def getDecreesList(self):
        fin  = open(str(self.node_id)+"_decrees_list.txt");
        decrees_list = []
        for i in xrange(0,self.lastAcceptedDecree):
            x,y = map(int, fin.readline().split());
            data = {"prev_ballot":int(x),"prev_vote":int(y)}
            decrees_list.append(data)
        fin.close();
        return decrees_list;

# WRITE VARIABLES TO DATASOURCE - LOCAL FILE SYSTEM
    def updateLastTried(self):
        fout  = open(str(self.node_id)+"_last_tried.txt","w"); fout.write(str(self.lastTried)); fout.close();
    def updateMaxBallot(self):
        fout  = open(str(self.node_id)+"_max_ballot.txt","w"); fout.write(str(self.maxBallot)); fout.close();

    def updateLastAcceptedDecree(self):
        fout  = open(str(self.node_id)+"_last_accepted_vote.txt","w"); fout.write(str(self.lastAcceptedDecree)); fout.close();

    def updateDecreesList(self):
        fout  = open(str(self.node_id)+"_decrees_list.txt","w");

        if len(self.decreesList) != self.lastAcceptedDecree:
            print "ERROR!!!!!! COUNT OF DECREES != LAST ACCEPTED DECREE"
            return

        for data in self.decreesList:
            fout.write(str(data["prev_ballot"])+" "+str(data["prev_vote"])+"\n");
        fout.close();

# heartbeat
    def heart_beat_pinger(self):
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        while True:
            time.sleep(heartBeatDelay)
            print "heart beat.. "
            for i in xrange(1,self.node_id):
                channel.basic_publish(exchange='',routing_key='heart_beat'+str(i),body="ASD")
            self.shouldFail()# probabilistic messenger failure
            # check if leader
            if self.isLeader():
                if self.wasLeader == False:
                    print "I'm a NEW leader !!!      #"+str(self.node_id)
                    self.wasLeader = True
                    self.next_ballot()
                else:
                    print "I'm leader       =      #"+str(self.node_id)
            else:
                print "I am not a leader :|       #"+str(self.node_id)
                self.wasLeader = False
        connection.close()

    def heart_beat_callback(self, channel, method_frame, header_frame, body):# on participant
        # print  "HEARTBEAT CALLBACK"
        currentTime = int(round(time.time() * 1000))
        # print "heartbeat_difference" + str(currentTime - self.lastTimeHeartBeat)
        self.lastTimeHeartBeat = int(round(time.time() * 1000))

    def isLeader(self):
        currentTime = int(round(time.time() * 1000))
        if (2*heartBeatDelay*1000) < (currentTime - self.lastTimeHeartBeat):
            return True
        #2 periods nobody checks me
        return False

#   probability to fail
    def shouldFail(self):
        return
        # TODO: create failure enviroment
        # if(randint(1,11) < 2):# small chance of failure
        #     print "Sleep of failure"+str(self.node_id)+" = 3 sec"
        #     time.sleep(3)
        #     self.lastTimeHeartBeat = int(round(time.time() * 1000))# just woke up, first assume you are not leader
# 1
    def next_ballot(self):#on leader
        print "start next ballot"
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        currentTime = int(round(time.time() * 1000))
        self.lastTimeCurRound = currentTime
        b = currentTime * 100 + self.node_id
        #self.lastTried + self.node_id # temporarily
        data = {'leader_id': self.node_id ,'b': int(b),"leader_n":self.lastAcceptedDecree}
        self.lastTried = b
        self.updateLastTried()
        self.ballotParticipants = list()
        for i in xrange(1,N+1):
            channel.basic_publish(exchange='', routing_key='next_ballot'+str(i),body=json.dumps(data))
        connection.close()
        t = threading.Timer(4.0, self.calculate_decrees) # check who responded with lastVote after 2 seconds
        t.start()

    def next_ballot_callback(self, channel, method_frame, header_frame, body):# on participant
        print "next_ballot_callback"
        data = json.loads(body)
        b = int(data['b'])
        if b < self.maxBallot:
            print 'ballot will be ignoreed #' + str(b) + " <  maxBallot " + str(self.maxBallot)
            return
        leader_n = int(data['leader_n'])
        self.ballotLeader = int(data['leader_id'])
        self.maxBallot = b
        self.updateMaxBallot()
        print "new max_ballot = " + str(self.maxBallot)
        self.last_vote(b,leader_n)
# 2
    def last_vote(self,b,leader_n):# on participant
        print "last_vote"

        # ,self.prevVote,self.prevBallot

        decrees_to_send = []
        if(self.lastAcceptedDecree > leader_n):
            for i in xrange(leader_n,self.lastAcceptedDecree):
                item = self.decreesList[i]
                decrees_to_send.append(item)

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        data = {'b': int(b),
                "participant_id":self.node_id,
                "participant_m":self.lastAcceptedDecree,
                "decrees_list":decrees_to_send}

        channel.basic_publish(exchange='', routing_key='last_vote'+str(self.ballotLeader),body=json.dumps(data))
        connection.close()

    def last_vote_callback(self, channel, method_frame, header_frame, body):# on leader
        print "last_vote_callback"
        data = json.loads(body)
        b = int(data['b'])
        if b != self.lastTried:
            print "ballot != lastTried"# response ballot is not equal to current one
            return
        currentTime = int(round(time.time() * 1000))
        if currentTime - self.lastTimeCurRound > 2*messageDelay*1000:
            print "Last_vote - message delayed"
            return

        participant_m = int(data['participant_m'])
        participantId = int(data['participant_id'])
        participant_decrees = data['decrees_list']

        self.ballotParticipants.append(data);

# LEADER chooses right answers
    def calculate_decrees(self):#
        print "calculate_decrees"
        if len(self.ballotParticipants) < N/2+1:
            print "DON'T HAVE A QUORUM"
            self.wasLeader = False # start new ballot on next iteration
            return;

        # self.decreesList[ 0 .. self.lastAcceptedDecree]
        max_m = 0
        for data in self.ballotParticipants:

            b = int(data['b'])
            if b != self.lastTried:
                print "calculate decrees : ballot != last_tried"
                return

            participantId = int(data['participant_id'])
            participant_m = int(data['participant_m'])
            if max_m < participant_m:
                max_m = participant_m

        for i in xrange(0,max_m):
            if(len(self.decreesList) < max_m):
                data = {"prev_ballot":int(0),"prev_vote":int(0)}
                self.decreesList.append(data)

        n = self.lastAcceptedDecree
        for data in self.ballotParticipants:
            participantId = int(data['participant_id'])
            participant_m = int(data['participant_m'])
            participant_decrees = data['decrees_list']

            for i in xrange(0,len(participant_decrees)):
                item = participant_decrees[i]
                prevVote = int(item['prev_vote'])
                prevBallot = int(item['prev_ballot'])
                ith = n + i

                currentItem = self.decreesList[ith]
                if prevVote == 0: continue
                if prevBallot > currentItem['prev_ballot']:
                    currentItem['prev_vote'] = prevVote

        print "old n = "+ str(n) + "  new learned n =  "+ str(self.lastAcceptedDecree)


        if(len(self.decreesList) + 1 > self.countTotalDecrees):
            print "WE HAVE SOLVED ALL PROBLEMS !!!"
            return

        # TODO: Master makes here decision!!!
        newDecree = randint(1,2) # 1 - alpha , 2 - betta
        data = {"prev_ballot":int(self.lastTried),
                "prev_vote":int(newDecree)}
        self.decreesList.append(data)
        self.lastAcceptedDecree = len(self.decreesList);

        print "I have a quorum of"+str(len(self.ballotParticipants)) + " made new decree = " + str(newDecree)+"  for  Q:" + str(len(self.decreesList))
        self.begin_ballot(self.lastTried)

# 3
    def begin_ballot(self,b):#on leader
        print "begin_ballot" + str(b % 1000) + " decrees_list :" + str(self.decreesList)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        self.ballotDecreeConfirmers = list()

        currentTime = int(round(time.time() * 1000))
        self.lastTimeDecreeVotedCurRound = currentTime

        data = {'leader_id': self.node_id ,'b': int(b), 'decrees_list': self.decreesList}
        for data_i in self.ballotParticipants:
            participantId = int(data_i['participant_id'])
            channel.basic_publish(exchange='', routing_key='begin_ballot'+str(participantId),body=json.dumps(data))
        connection.close()
        t = threading.Timer(4.0, self.ballot_confirmers) # check who responded with lastVote after 5 seconds
        t.start()

    def begin_ballot_callback(self, channel, method_frame, header_frame, body):# on participant
        print "begin_ballot_callback"
        data = json.loads(body)
        b = int(data['b'])
        if b != self.maxBallot:
            print 'ballot != maxBallot' + str(b)
            return
        # print "data" + str(data)

        from_leader_decrees = data['decrees_list']
        self.decreesList = from_leader_decrees
        self.lastAcceptedDecree = len(self.decreesList)

        self.updateLastAcceptedDecree()
        self.updateDecreesList()

        print "FOR BALLOT #" + str(b % 1000) + " ON NODE #" + str(self.node_id)+  " VOTE  : " +\
              str(from_leader_decrees[len(from_leader_decrees)-1]) +" !!!"
        self.voted(b)

    def voted(self,b):#on participant
        print "voted"
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        data = {'b': int(b),'participant_id':int(self.node_id)}
        channel.basic_publish(exchange='', routing_key='voted'+str(self.ballotLeader),body=json.dumps(data))
        connection.close()

    def voted_callback(self, channel, method_frame, header_frame, body):# on leader
        print "voted_callback"
        data = json.loads(body)
        b = int(data['b'])
        if b != self.lastTried:
            print 'voted_callback: ballot != lastTried ' + str(b)
            return

        currentTime = int(round(time.time() * 1000))
        if currentTime - self.lastTimeDecreeVotedCurRound > 2*messageDelay*1000:
            print "Voted - message delayed"
            return

        participantId = int(data['participant_id'])
        print "participant = "+str(participantId)+"confirming decree in ballot b = "+ str(b)
        self.ballotDecreeConfirmers.append(data)

# LEARNER method
    def ballot_confirmers(self):
        print "ballot_confirmers_count"
        for data in self.ballotDecreeConfirmers:
            b = int(data['b'])
            if b != self.lastTried: return
            participantId = int(data['participant_id'])
            print " PARTICIPANT #"+str(participantId)+" VOTED IN BALLOT #"+ str(b)

        # TODO: modification for Multi-Paxos
        # continue without next ballots here with a stable leader
        self.calculate_decrees()
