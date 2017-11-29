## Traffic Reduction Apps based on DFT

//TODO

This traffic reduction app use infos from the optimizer, add tag
according to the rules defined by Designer/User

The main problem of designing such engine is that we should make
the whole framework programmable. 

So where should we put all the user-define code???
so we have two things that should be defined by the programmer
first is the tag policy of the system(partition scheme produced
by the system) and the selection of the policies(which partition
policies to be encored)

//TODO We should also need to check if a key could be used
to partition(when there are many machines... and this scheme
will only generate few partitions)