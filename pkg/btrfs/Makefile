.DEFAULT_GOAL = all
.SECONDARY:
.DELETE_ON_ERROR:

btrfsitem/items.txt: btrfsitem $(wildcard btrfsitem/item_*.go) $(MAKEFILE_LIST)
	sed -En 's,^type (\S+) .* // (.*=.*),\1 \2,p' $(filter btrfsitem/item_%.go,$^) | while read -r typ keys; do for key in $$keys; do echo "$$key" "$$typ"; done; done >$@
files += btrfsitem/items.txt

btrfsitem/items.go: btrfsitem/items.txt $(MAKEFILE_LIST)
	{ \
	  echo 'package $(@D)'; \
	  echo 'import "lukeshu.com/btrfs-tools/pkg/btrfs/internal"'; \
	  echo 'type Type = internal.ItemType'; \
	  echo 'const ('; \
	  sed -E 's,(.*)=(.*) (.*),\1_KEY=internal.\1_KEY,' $<; \
	  echo ')'; \
	} | gofmt >$@
files += btrfsitem/items.go

internal/itemtype.go: btrfsitem/items.txt $(MAKEFILE_LIST)
	{ \
	  echo 'package $(@D)'; \
	  echo 'import "fmt"'; \
	  echo 'type ItemType uint8'; \
	  echo 'const ('; \
	  sed -E 's,(.*)=(.*) (.*),\1_KEY=ItemType(\2),' $<; \
	  echo ')'; \
	  echo 'func (t ItemType) String() string {'; \
	  echo '  names := map[ItemType]string{'; \
	  sed -E 's@(.*)=(.*) (.*)@\1_KEY: "\1",@' $<; \
	  echo '  }'; \
	  echo '  if name, ok := names[t]; ok {'; \
	  echo '    return name'; \
	  echo '  }'; \
	  echo '  return fmt.Sprintf("%d", t)'; \
	  echo '}'; \
	} | gofmt >$@
files += internal/itemtype.go

all: $(files)
.PHONY: all

clean:
	rm -f -- $(files)
.PHONY: all
