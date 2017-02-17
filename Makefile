SOURCE_DIR := src

# COMPILER := icc
COMPILER := g++

# release
BINARY_DIR := bin/lrelease
CPPFLAGS := -Wall -g -O3 
#CPPFLAGS := -g
LIBS :=  -lpthread  

# debug
# BINARY_DIR := bin/ldebug
# CPPFLAGS := -g -fPIC -DDEBUG
COMPILE.cpp = $(COMPILER) $(CFLAGS) $(CPPFLAGS) -c
LINK.cpp = $(COMPILER) $(LIBS) -g -O3

COMPILE.cilk = cilk++ $(CFLAGS) $(CPPFLAGS) -c
#LINK.cilk = cilk++ -O1 -g -lcilkutil $(CPPFLAGS)
LINK.cilk = cilk++ -g -lcilkutil $(CPPFLAGS)

%.o: %.cpp
	#$(call make-depend,$<,$@,$(subst .o,.d,$@))
	$(COMPILE.cpp) $< -o $@


# $(call source-dir-to-binary-dir, directory-list)
source-dir-to-binary-dir = $(addprefix $(BINARY_DIR)/,$1)

# $(call source-to-object, source-file-list)
source-to-object = $(call source-dir-to-binary-dir, $(subst .cpp,.o,$1))

# $(subdirectory)
subdirectory = $(patsubst %/module.mk,%,                \
                 $(word                                 \
                   $(words $(MAKEFILE_LIST)),$(MAKEFILE_LIST)))

# $(call make-depend,source-file,object-file,depend-file)
define make-depend
	g++ -MM    \
         -MF$3         \
         -MP            \
         -MT$2         \
         $(CFLAGS)      \
         $(CPPFLAGS)    \
         $(TARGET_ARCH) \
         $1
endef

        
# $(call make-library, library-name, source-file-list)
define make-library
  libraries += $(BINARY_DIR)/$1
  sources   += $2

  $(BINARY_DIR)/$1: $(call source-dir-to-binary-dir, $(subst .cpp,.o,$2))
	$(AR) $(ARFLAGS) $$@ $$^

endef


# $(call make-program, program-name, library-list, source-file-list)
define make-program
  programs  += $(BINARY_DIR)/$1
  sources   += $3
  
  $(BINARY_DIR)/$1: $(call source-dir-to-binary-dir, $(subst .cpp,.o,$3) $2 )
	$(LINK.cpp) -o $$@ $$^ -lpthread

endef

# $(call make-program-cilk, program-name, library-list, source-file-list)
define make-program-cilk
  programs  += $(BINARY_DIR)/$1
  sources   += $3
  
  $(BINARY_DIR)/$1: $(call source-dir-to-binary-dir, $(subst .cpp,.o,$3) $2 )
	cilk++ $$^ -g -O3 -lcilkutil -o $$@

endef


# $(compile-rules)
define compile-rules
  $(foreach f,$(local_src),$(call one-compile-rule,$(call source-to-object,$f),$f))

endef



# $(call one-compile-rule, binary-file, source-file)
define one-compile-rule
  $1: $2
	$(call make-depend,$2,$1,$(subst .o,.d,$1))
	$(COMPILE.cpp) -o $1 $2

endef

# $(compile-rules-cilk)
define compile-rules-cilk
  $(foreach f,$(local_src),$(call one-compile-rule-cilk,$(call source-to-object,$f),$f))

endef

# $(call one-compile-rule-cilk, binary-file, source-file)
define one-compile-rule-cilk
  $1: $2
	$(call make-depend,$2,$1,$(subst .o,.d,$1))
	$(COMPILE.cilk) -o $1 $2

endef


modules      := TripleBit Matrix Algorithm
programs     := 
libraries    :=
sources      :=

objects      =  $(call source-to-object,$(sources))
dependencies =  $(subst .o,.d,$(objects))

include_dirs := TripleBit Matrix /usr/include/i386-linux-gnu
CPPFLAGS += $(addprefix -I ,$(include_dirs))

MKDIR := mkdir -p
MV    := mv -f
RM    := rm -f
SED   := sed
TEST  := test


create-output-directories := \
  $(shell for f in $(call source-dir-to-binary-dir,$(modules));   \
          do                                                      \
            $(TEST) -d $$f || $(MKDIR) $$f;                       \
          done)


all:

include $(addsuffix /module.mk,$(modules))


.PHONY: all

all: $(programs)

.PHONY: libraries

libraries: $(libraries)

.PHONY: clean

clean:
	$(RM) -r $(BINARY_DIR)


	
ifneq "$(MAKECMDGOALS)" "clean"
  -include $(dependencies)
endif

