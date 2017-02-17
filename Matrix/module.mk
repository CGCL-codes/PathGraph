local_src := $(wildcard $(subdirectory)/*.cpp)

$(eval $(call make-library,libmatrix.a,$(local_src),))

$(eval $(call compile-rules))
