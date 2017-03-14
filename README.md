# Distributed-database_Parallel-processing

The rating.dat file contains 10 million ratings and 100,000 tag applications applied to 10,000 movies by
72,000 users. Each line of this file represents one rating of one movie by one user, and has the following
format:

UserID::MovieID::Rating::Timestamp

The lines within this file are ordered first by UserID, then, within user, by MovieID. Ratings are made
on a 5-star scale, with half-star increments. Timestamps represent seconds since midnight Coordinated
Universal Time (UTC) of January 1, 1970. A sample of the file contents is given below:

1::122::5::838985046
1::185::5::838983525
1::231::5::838983392

Task 1:

A Python function Load Ratings() that takes a file system path that contains the rating.dat
file as input. Load Ratings() then load the rating.dat content into a table (saved in PostgreSQL)
named Ratings that has the following schema
UserID - MovieID - Rating

A Python function Range Partition() that takes as input: (1) the Ratings table stored in
PostgreSQL and (2) an integer value N; that represents the number of partitions. Range Partition()
then generates N horizontal fragments of the Ratings table and store them in PostgreSQL. The algo-
rithm should partition the ratings table based on N uniform ranges of the Rating attribute.

A Python function RoundRobin Partition() that takes as input: (1) the Ratings table
stored in PostgreSQL and (2) an integer value N; that represents the number of partitions. The
function then generates N horizontal fragments of the Ratings table and stores them in PostgreSQL.
The algorithm should partition the ratings table using the round robin partitioning approach (explained
in class).

A Python function RoundRobin Insert() that takes as input: (1) Ratings table stored in
PostgreSQL, (2) UserID, (3) ItemID, (4) Rating. RoundRobin Insert() then inserts a new tuple in
the right fragment (of the partitioned ratings table) based on the round robin approach.

A Python function Range Insert() that takes as input: (1) Ratings table stored in Post-
greSQL (2) UserID, (3) ItemID, (4) Rating. Range Insert() then inserts a new tuple in the correct
fragment (of the partitioned ratings table) based upon the Rating value.

A Python function Delete Partitions() that deletes all generated partitions as well as
any metadata related to the partitioning scheme.

Task 2:

RangeQuery() – A Python function RangeQuery that takes as input: (1) Ratings table
stored in PostgreSQL, (2) RatingMinValue (3) RatingMaxValue (4)
openconnection. RangeQuery() then returns all tuples for which the rating value is larger than or
equal to RatingMinValue and less than or equal to RatingMaxValue. The returned tuples should be stored in a text file, named RangeQueryOut.txt

PartitionName, UserID, MovieID, Rating

Example:
RangeRatingsPart0,1,377,0.5
RoundRobinRatingsPart1,1,377,0.5

PointQuery() –A Python function PointQuery that takes as input: (1) Ratings table
stored in PostgreSQL, (2) RatingValue. (3) openconnection. PointQuery() then returns all tuples for which the rating value is equal to RatingValue. The returned tuples should be stored in a text file, named PointQueryOut.txt

PartitionName, UserID, MovieID, Rating
Example
RangeRatingsPart3,23,459,3.5
RoundRobinRatingsPart4,31,221,0

Task 3:






