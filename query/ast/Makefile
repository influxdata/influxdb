SUBDIRS = asttest

subdirs: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

all: $(SUBDIRS)

clean: $(SUBDIRS)


.PHONY: all clean subdirs $(SUBDIRS)
