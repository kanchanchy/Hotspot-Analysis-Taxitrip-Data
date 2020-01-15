# Hotspot-Analysis-Taxitrip-Data

This repository performs hotzone and hotspot analysis on New York taxi trip data. The repository has two parts.

# 1. Part-1:
It writes two User Defined Functions ST_Contains and ST_Within in SparkSQL and use them to do four spatial queries:

    Range query: Use ST_Contains. Given a query rectangle R and a set of points P, find all the points within R.
    
    Range join query: Use ST_Contains. Given a set of Rectangles R and a set of Points S, find all (Point, Rectangle) pairs such that the     point is within the rectangle.
    
    Distance query: Use ST_Within. Given a point location P and distance D in km, find all points that lie within a distance D from P
    
    Distance join query: Use ST_Within. Given a set of Points S1 and a set of Points S2 and a distance D in km, find all (s1, s2) pairs       such that s1 is within a distance D from s2 (i.e., s1 belongs to S1 and s2 belongs to S2).


# 2. Part-2
It performs hotzone analysis and hotspot analysis on New York taxi Trip data.

# Hot zone analysis

It performs a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle is obtained. The hotter rectangle means that it include more points. So the operation is to calculate the hotness of all the rectangles.
