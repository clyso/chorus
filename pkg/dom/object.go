package dom

import (
	"fmt"
)

type Object struct {
	Bucket  string
	Name    string
	Version string
}

func (o Object) Key() string {
	if o.Version == "" {
		return fmt.Sprintf("%s:%s", o.Bucket, o.Name)
	}
	return fmt.Sprintf("%s:%s:%s", o.Bucket, o.Name, o.Version)
}

func (o Object) ACLKey() string {
	return o.Key() + ":a"
}

func (o Object) TagKey() string {
	return o.Key() + ":t"
}
