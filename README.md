# PathGraph
PathGraph, a path-centric graph processing system for fast iterative computation on large graphs with billions of edges.

Large scale graph analysis applications typically involve datasets of massive scale. Most of existing approaches address the iterative graph computation problem by programming and executing graph computation using either vertex centric or edge centric approaches. We develop a path-centric graph processing system PathGraph for fast iterative computation on large graphs with billions of edges.

Our development is motivated by our observation that most of the iterative graph computation algorithms share three common processing requirements: (1) For each vertex to be processed, we need to examine all its outgoing or incoming edges and all of its neighbor vertices. (2) The iterative computation for each vertex will converge only after completing the traversal of the graph through the direct and indirect edges connecting to this vertex. (3) The iterative computation of a graph converges when all vertices have completed their iterative computation.

PathGraph
(c) 2013 Massive Data Management Group @ SCTS & CGCL. 
	Web site: http://grid.hust.edu.cn/triplebit

This work is licensed under the Creative Commons
Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
or send a letter to Creative Commons, 171 Second Street, Suite 300,
San Francisco, California, 94105, USA.


Dependency:
-----------
Please install boost and cilk, you can use the following versions.

boost_1_46_1.tar.gz
cilk-8503-i686-32bit.release.tar.gz or cilk-8503-x86-64bit.release.tar.gz

next lines are the way to install boost:
# tar -zxvf boost_1_46_1.tar.gz
# cd boost_1_46_1
# ./bootstrap.sh
# ./bjam
# cp -r boost/ /usr/local/include/

next lines are the way to install cilk:
# tar -zxvf cilk-8503-i686-32bit.release.tar.gz
# cp -r cilk/ /usr/local/

Building:
---------

PathGraph must be built using GNU make and reasonable C++ and Cilk++ compilers. Ideally simple

    export PATH=/usr/local/cilk/bin/:$PATH
    make

is enough, it will build several high-level executables in bin/lrelease/.

Using:
------

PathGraph currently includes two high-level builder executables. They are used to build a PathGraph
database from an adjacency list dataset, you can choose one of them, the third parameter is the
total vertex number of the dataset:

    ./bin/lrelease/matrix_builder <Dataset> <Database Directory> <Vertex Number>
        or
    ./bin/lrelease/graph_builder <Dataset> <Database Directory> <Vertex Number>
   
    examples: 
	    ./bin/lrelease/matrix_builder Dataset/amazon-2008.txt amazon/ 735322
        ./bin/lrelease/graph_builder Dataset/amazon-2008.txt amazon/ 735322


After loading, there are several applications for test, like pageRank, spmv, wcc, bfs_forest, and so on.
They all realized in two ways, parallelism with cilk scheduler, or parallelism with work stealing 
scheduler offered by the PathGraph itself:

    cilk way:
        ./bin/lrelease/pageRank <Database Directory> <iterate times> <printtop>
	    ./bin/lrelease/spmv <Database Directory>
	    ./bin/lrelease/par_wcc <Database Directory> <printtop>
	    ./bin/lrelease/par_bfs_forest <Database Directory>
   
    work stealing way offered by PathGraph:
        ./bin/lrelease/pageRank_ws <Database Directory> <iterate times> <printtop>
	    ./bin/lrelease/spmv_ws <Database Directory>
	    ./bin/lrelease/par_wcc_ws <Database Directory> <printtop>
	    ./bin/lrelease/par_bfs_forest_ws <Database Directory>
		
	examples: 
		./bin/lrelease/pageRank amazon/ 4 20
		./bin/lrelease/pageRank_ws amazon/ 4 20
		
	    ./bin/lrelease/spmv amazon/
		./bin/lrelease/spmv_ws amazon/
		
	    ./bin/lrelease/par_wcc amazon/ 20
		./bin/lrelease/par_wcc_ws amazon/ 20
		
	    ./bin/lrelease/par_bfs_forest amazon/
		./bin/lrelease/par_bfs_forest_ws amazon/
	
You can include your own applications, for more detail, please take a glance at the 
source code in Matrix/AppExample.cpp and Algorithm/.


Publication:

Pingpeng Yuan, Pu Liu, Buwen Wu, Ling Liu, Hai Jin, and Wenya Zhang. TripleBit: a Fast and Compact System for Large Scale RDF Data. PVLDB, 6(7):517-528, 2013.
Buwen Wu, Yongluan Zhou, Pingpeng Yuan, Hai Jin, Ling Liu. SemStore: A Semantic-Preserving Distributed RDF Triple Store. CIKM 2014, Shanghai, China, Nov 3-7, 2014.
Pingpeng Yuan, Wenya Zhang, Changfeng Xie, Ling Liu, Hai Jin, Kisung Lee. Fast Iterative Graph Computation: A Path Centric Approach. SC 2014, New Orleans, USA, Nov.16-21, 2014.
Buwen Wu, Yongluan Zhou, Pingpeng Yuan, Ling Liu and Hai Jin. Scalable SPARQL Querying using Path Partitioning. ICDE 2015, Seoul, Korea, April 13-16, 2015.

Feedback:

If you have comments, questions, or suggestions regarding TripleBit, SemStore or PathGraph, please email Pingpeng Yuan
