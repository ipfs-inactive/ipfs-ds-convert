package main

import (
	"os"
	"path"

	"github.com/ipfs/ipfs-ds-convert/convert"
	homedir "github.com/mitchellh/go-homedir"
	cli "gx/ipfs/QmVcLF2CgjQb5BWmYFWsDfxDjbzBfcChfdHRedxeL3dV4K/cli"
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
		convert.Log.Fatal(err)
	}
}

var ConvertCommand = cli.Command{
	Name:  "convert",
	Usage: "convert datastore setup",
	Description: `'convert' converts existing ipfs datastore setup to another based on the
ipfs configuration and repo specs.

IPFS_PATH environmental variable is respected
	`,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name: "keep",
			Usage: "don't remove backup files after successful conversion",
		},
	},
	Action: func(c *cli.Context) error {
		baseDir, err := getBaseDir()
		if err != nil {
			return err
		}

		return convert.Convert(baseDir, c.Bool("keep"))
	},
}

//TODO: Patch config util command

//TODO: revert command

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
