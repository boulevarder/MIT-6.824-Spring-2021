### electionTimeout使用time.Timer遇到的问题
- 会出现这样的情况: 
    - 收到一个RequestVote RPC或AppendEntries RPC需要重置了election time, 然而收到报文与重置超时时间是有时间差的, 这个时间差里election timeout超时了, 就导致了既回应了这些RPC, 又变成了candidate。