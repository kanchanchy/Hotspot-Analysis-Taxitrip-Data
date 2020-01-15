# Hotspot-Analysis-Taxitrip-Data

This repository performs hotzone and hotspot analysis on New York taxi trip data. The repository has two parts.

# 1. Part-1:
It writes two User Defined Functions ST_Contains and ST_Within in SparkSQL and use them to do four spatial queries:

    Range query: Use ST_Contains. Given a query rectangle R and a set of points P, it finds all the points within R.
    
    Range join query: Use ST_Contains. Given a set of Rectangles R and a set of Points S, it finds all (Point, Rectangle) pairs such that the point is within the rectangle.
    
    Distance query: Use ST_Within. Given a point location P and distance D in km, it finds all points that lie within a distance D from P
    
    Distance join query: Use ST_Within. Given a set of Points S1 and a set of Points S2 and a distance D in km, it finds all (s1, s2) pairs such that s1 is within a distance D from s2 (i.e., s1 belongs to S1 and s2 belongs to S2).


# 2. Part-2:
It performs hotzone analysis and hotspot analysis on New York taxi Trip data.

# Hotzone analysis

It performs a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle is obtained. The hotter rectangle means that it include more points. So the operation is to calculate the hotness of all the rectangles. The output is all zones with their count, sorted by "rectangle" string in an ascending order.

# Hotspot Analysis

It implements a Spark program to calculate the Getis-Ord statistic of NYC Taxi Trip datasets. It is hot cell analysis. The input is a monthly taxi trip dataset from 2009 to 2012. Each cell unit size is 0.01 * 0.01 in terms of latitude and longitude degrees.
Time Step size is 1 day. The first day of a month is step 1. Every month has 31 days.
Only Pick-up Location is considered. The output is the coordinates of top 50 hotest cells sorted by their G score in a descending order.

