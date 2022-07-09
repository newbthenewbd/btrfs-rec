module lukeshu.com/btrfs-tools

go 1.18

require (
	github.com/datawire/dlib v1.3.0
	github.com/davecgh/go-spew v1.1.1
	github.com/hashicorp/golang-lru v0.5.4
	github.com/jacobsa/fuse v0.0.0-20220702091825-13117049f383
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.7.1
	golang.org/x/exp v0.0.0-20220518171630-0b5c67f07fdf
	golang.org/x/text v0.3.7
)

require (
	github.com/konsorten/go-windows-terminal-sequences v1.0.3 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)

replace github.com/jacobsa/fuse => github.com/lukeshu/jacobsa-fuse v0.0.0-20220706162300-f42bfdd0fc53
