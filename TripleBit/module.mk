local_src := $(wildcard $(subdirectory)/*.cpp)

$(eval $(call make-library,libtriplebit.a,$(local_src)))

$(eval $(call compile-rules))
