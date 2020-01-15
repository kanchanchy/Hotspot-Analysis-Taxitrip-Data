# Hotspot-Analysis-Taxitrip-Data

This repository performs hotzone and hotspot analysis on New York taxi trip data. The repository has two parts.

1. Part-1 writes two User Defined Functions ST_Contains and ST_Within in SparkSQL and use them to do four spatial queries:

    Range query: Use ST_Contains. Given a query rectangle R and a set of points P, find all the points within R.
    
    Range join query: Use ST_Contains. Given a set of Rectangles R and a set of Points S, find all (Point, Rectangle) pairs such that the     point is within the rectangle.
    
    Distance query: Use ST_Within. Given a point location P and distance D in km, find all points that lie within a distance D from P
    
    Distance join query: Use ST_Within. Given a set of Points S1 and a set of Points S2 and a distance D in km, find all (s1, s2) pairs       such that s1 is within a distance D from s2 (i.e., s1 belongs to S1 and s2 belongs to S2).
