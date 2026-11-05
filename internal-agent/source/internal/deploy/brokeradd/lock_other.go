//go:build !linux

package brokeradd

import "fmt"

func lock(path string) (func(), error) { return nil, fmt.Errorf("broker operations require Linux") }
