package api

type Config struct {
	Enabled  bool `yaml:"enabled"`
	GrpcPort int  `yaml:"grpcPort"`
	HttpPort int  `yaml:"httpPort"`
	Secure   bool `yaml:"secure"`
}
