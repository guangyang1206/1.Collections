1. We have a data service which holds 4 cache. When the systems starts up these 4 caches are loaded via 4 threads.
All data requests should be blocked until unless all 4 caches are loaded. So say if there are 10 threads requesting 
data from it while the cache load are not finished, then all of them should wait. As soon as caches are loaded all 
requests should be serviced.

2. We have 4 different java threads which are loading data from different systems into db. when all data are loaded,
we need to run a new java thread which will do some operation fetching the data.
We have a time constraint too.

3. We have a producer who is producing at very slow rate. So consumer threads are getting bolcked. Say 10 consumer threads
are waiting. When producers are ready with data we need to make sure that consumers threads gets chance to resume but in
the same order as they were blocked.

4. Run 3 threads print in order. Say thread 1 is printing A and thread 2 is printing B ad thread 3 is printing C. Synchronize threads
such that we get input like below.

AAAA
BBBB
CCCC
AAAA
BBBB
CCCC
.........

5. Say we have a cache in form of HashMap where the key is employee name and the value is manager name.
Now we want access this in multithreaded environment in most optimum way such that we always get correct
data. We can not use synchronized map as even all reads have go through lock acquisition overhead.
If we use ConcurrentHashMap then when we read, it does not garuntee we are reading correct value. Because
if one thread is updating 1 employee details and same time if another thread tries to read it will see old
value. So we need some different solution.