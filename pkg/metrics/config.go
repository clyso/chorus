package metrics

type Config struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}
