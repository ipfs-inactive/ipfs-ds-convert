package main

import (
	"errors"
	"os"
	"path"

	cli "github.com/codegangsta/cli"
	homedir "github.com/mitchellh/go-homedir"
)

const (
	DefaultPathName   = ".ipfs"
	DefaultPathRoot   = "~/" + DefaultPathName
	DefaultConfigFile = "config"
	EnvDir            = "IPFS_PATH"
)

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "verbose",
			Usage: "print verbose logging information",
		},
	}

	app.Before = func(c *cli.Context) error {
		return nil
	}

	app.Commands = []cli.Command{
		ConvertCommand,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

var ConvertCommand = cli.Command{
	Name:      "convert",
	Usage:     "convert datastore setup",
	ArgsUsage: "[new config]",
	Description: `'convert' converts existing ipfs datastore setup to another based on the
provided configuration, moving all data in the process.

[new config] is a json file containing only the datastore part of ipfs
configuration

IPFS_PATH environmental variable is respected
	`,
	Action: func(c *cli.Context) error {
		if len(c.Args()) != 1 {
			return errors.New("Invalid number of arguments")
		}

		baseDir, err := getBaseDir()
		if err != nil {
			return err
		}

		newConfigPath, err := homedir.Expand(c.Args()[0])
		if err != nil {
			return err
		}

		return Convert(baseDir, newConfigPath)
	},
}

func getBaseDir() (string, error) {
	baseDir := os.Getenv(EnvDir)
	if baseDir == "" {
		baseDir = DefaultPathRoot
	}

	baseDir, err := homedir.Expand(baseDir)
	if err != nil {
		return "", err
	}

	configFile := path.Join(baseDir, DefaultConfigFile)

	_, err = os.Stat(configFile)
	if err != nil {
		return "", err
	}

	return baseDir, nil
}
