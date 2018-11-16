
SUBDIRS = promql

subdirs: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

all: $(SUBDIRS)

.PHONY: all subdirs $(SUBDIRS)

