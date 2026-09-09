package lock

import "time"

type Config struct {
	Driver string      `mapstructure:"driver"`
	Config RedisConfig `mapstructure:"config"`
}

type RedisConfig struct {
	Addrs        []string      `mapstructure:"addrs"`
	Username     string        `mapstructure:"username"`
	Password     string        `mapstructure:"password"`
	DB           int           `mapstructure:"db"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}
