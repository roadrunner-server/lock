package lock

import (
	"errors"
	"fmt"
	"time"
)

type Config struct {
	Driver string      `mapstructure:"driver"`
	Config RedisConfig `mapstructure:"config"`
}

type RedisConfig struct {
	Addrs            []string      `mapstructure:"addrs"`
	Username         string        `mapstructure:"username"`
	Password         string        `mapstructure:"password"`
	DB               int           `mapstructure:"db"`
	MasterName       string        `mapstructure:"master_name"`
	SentinelPassword string        `mapstructure:"sentinel_password"`
	PoolSize         int           `mapstructure:"pool_size"`
	DialTimeout      time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout      time.Duration `mapstructure:"read_timeout"`
	WriteTimeout     time.Duration `mapstructure:"write_timeout"`
	TLSConfig        *TLSConfig    `mapstructure:"tls"`
}

func (c *RedisConfig) InitDefaults() {
	if c.Addrs == nil {
		c.Addrs = []string{"127.0.0.1:6379"}
	}
}

func (c *RedisConfig) Validate() error {
	if len(c.Addrs) == 0 {
		return errors.New("addrs must not be empty")
	}
	// go-redis builds a cluster client for more than one address. Cluster options carry no database.
	if len(c.Addrs) > 1 && c.MasterName == "" && c.DB != 0 {
		return errors.New("db must be 0 with more than one address because the cluster client uses database 0")
	}
	// A negative dial timeout gives the dialer a deadline in the past. Every dial fails at once.
	if c.DialTimeout < 0 {
		return errors.New("dial_timeout must not be negative")
	}
	// A negative read or write timeout removes the deadline from each command.
	if c.ReadTimeout < 0 {
		return errors.New("read_timeout must not be negative")
	}
	if c.WriteTimeout < 0 {
		return errors.New("write_timeout must not be negative")
	}
	if c.PoolSize < 0 {
		return fmt.Errorf("pool_size must not be negative, got %d", c.PoolSize)
	}
	if c.TLSConfig != nil && (c.TLSConfig.Cert == "") != (c.TLSConfig.Key == "") {
		return errors.New("tls cert and key must be set together")
	}
	return nil
}
