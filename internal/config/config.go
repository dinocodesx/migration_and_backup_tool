package config

// DBConfig holds connection details for a database.
type DBConfig struct {
	Type     string            `yaml:"type"`
	Host     string            `yaml:"host"`
	Port     int               `yaml:"port"`
	User     string            `yaml:"user"`
	Password string            `yaml:"password"`
	Database string            `yaml:"database"`
	Params   map[string]string `yaml:"params"`
}
