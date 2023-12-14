package standalone

import (
	"fmt"
	"github.com/clyso/chorus/pkg/config"
	"gopkg.in/yaml.v3"
)

func PrintConfig(src ...config.Src) error {
	conf, err := GetConfig(src...)
	if err != nil {
		return err
	}
	data, err := yaml.Marshal(conf)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
