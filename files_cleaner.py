N = 5;

print "create a clean DataStorage files with default values"

for node_id in xrange(1,N+1):
    fout  = open(str(node_id)+"_last_tried.txt","w");# last_tried
    fout.write(str(0))
fout.close()

for node_id in xrange(1,N+1):
    fout  = open(str(node_id)+"_max_ballot.txt","w");# max_ballot
    fout.write(str(0))
fout.close()

for node_id in xrange(1,N+1):
    fout  = open(str(node_id)+"_last_accepted_vote.txt","w");# n
    fout.write(str(0))
fout.close()

for node_id in xrange(1,N+1):
    fout  = open(str(node_id)+"_decrees_list.txt","w");# pairs of (decree,ballot number) for 1 .. n
    fout.write(str(0)+" "+str(0))
fout.close()

