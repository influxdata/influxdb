package api

import (
	"fmt"
	"net/url"
	"os"
	fp "path/filepath"
)

type Config struct {
	BindAddr string `toml:"bind_addr"`
	BoltFile string
}

func NewConfig() Config {
	return Config{
		BindAddr: "http://localhost:9999",
		BoltFile: "$HOME/.influxdbv2/influxdb.bolt",
	}
}

func (c *Config) WithBindAddr(addr string) Config {
	c.BindAddr = addr
	return *c
}

func (c *Config) WithBoltFile(path string) Config {
	c.BoltFile = path
	return *c
}

func (c *Config) Validate() error {
	// confirm that BindAddr is a valid URL
	if err := c.ValidateBindAddr(); err != nil {
		return err
	}

	// confirm that BoltPath is a valid path
	if err := c.ValidateBoltPath(); err != nil {
		return err
	}

	return nil
}

func (c *Config) ValidateBindAddr() error {
	if _, err := url.ParseRequestURI(c.BindAddr); err != nil {
		return err
	}
	return nil
}

func (c *Config) ValidateBoltPath() error {
	// Make sure path is a valid file path, possibly with the file package
	if _, err := os.Stat(c.BoltFile); os.IsNotExist(err) {
		return err
	}

	if fp.Ext(c.BoltFile) != ".bolt" {
		return fmt.Errorf("%s is not a valid bolt file", c.BoltFile)
	}

	return nil
}
