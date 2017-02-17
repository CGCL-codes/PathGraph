local_src := $(wildcard $(subdirectory)/*.cpp)

$(eval $(call make-program-cilk,matrix_builder,libmatrix.a libtriplebit.a,Algorithm/matrix_builder.cpp))

$(eval $(call make-program-cilk,graph_builder,libmatrix.a libtriplebit.a,Algorithm/graph_builder.cpp))

$(eval $(call make-program-cilk,pageRank,libmatrix.a libtriplebit.a,Algorithm/pagerank.cpp))

$(eval $(call make-program-cilk,pageRank_ws,libmatrix.a libtriplebit.a,Algorithm/pagerank_workstealing.cpp))

$(eval $(call make-program-cilk,spmv,libmatrix.a libtriplebit.a,Algorithm/spmv.cpp))

$(eval $(call make-program-cilk,spmv_ws,libmatrix.a libtriplebit.a,Algorithm/spmv_workstealing.cpp))

$(eval $(call make-program-cilk,par_bfs_forest,libmatrix.a libtriplebit.a,Algorithm/parallel_bfs_forest.cpp))

$(eval $(call make-program-cilk,par_bfs_forest_ws,libmatrix.a libtriplebit.a,Algorithm/parallel_bfs_forest_workstealing.cpp))

$(eval $(call make-program-cilk,par_wcc,libmatrix.a libtriplebit.a,Algorithm/parallel_wcc.cpp))

$(eval $(call make-program-cilk,par_wcc_ws,libmatrix.a libtriplebit.a,Algorithm/parallel_wcc_workstealing.cpp))

$(eval $(call compile-rules))
