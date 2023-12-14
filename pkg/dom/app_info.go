package dom

type AppInfo struct {
	Version string `yaml:"version"`
	Commit  string `yaml:"commit"`
	App     string `yaml:"app"`
	AppID   string `yaml:"appID"`
}
