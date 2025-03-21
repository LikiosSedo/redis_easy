SET course1 DB
SET course2 ML EX 60
TTL course2
LPUSH mylist alpha beta gamma
LRANGE mylist 0 -1
LPOP mylist
LRANGE mylist 0 -1
SADD courses DB ML IoT
SMEMBERS courses
SREM courses ML
SMEMBERS courses
HSET grade:db student1 90
HSET grade:db student2 85
HGET grade:db student1
LBADD student1 90
LBADD student2 95
LBADD student3 100
LBTOP 3
QUIT
