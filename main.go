package main

import (
	"os"
	"path"

	"github.com/ipfs/ipfs-ds-convert/convert"
	"github.com/ipfs/ipfs-ds-convert/repo"
	"github.com/ipfs/ipfs-ds-convert/revert"
	homedir "github.com/mitchellh/go-homedir"

	cli "github.com/codegangsta/cli"
)

const (
	DefaultPathName   = ".ipfs"
	DefaultPathRoot   = "~/" + DefaultPathName
	DefaultConfigFile = "config"
	EnvDir            = "IPFS_PATH"
)

func main() {
	run(os.Args)
}

func run(args []string) {
	app := cli.NewApp()

	app.Version = repo.ToolVersion

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
		RevertCommand,
		CleanupCommand,
	}

	if err := app.Run(args); err != nil {
		convert.Log.Fatal(err)
	}
}

var ConvertCommand = cli.Command{
	Name:  "convert",
	Usage: "convert datastore ",
	Description: `'convert' converts existing ipfs datastore setup to another based on the
ipfs configuration and repo specs.

Note that depending on configuration you are converting to up to double the
disk space may be required.

If you have any doubts about your configuration, run the tool conversion with
--keep option enabled

IPFS_PATH environmental variable is respected
	`,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "keep",
			Usage: "don't remove backup files after successful conversion",
		},
	},
	Action: func(c *cli.Context) error {
		baseDir, err := getBaseDir()
		if err != nil {
			convert.Log.Fatal(err)
		}

		err = convert.Convert(baseDir, c.Bool("keep"))
		if err != nil {
			convert.Log.Fatal(err)
		}
		return err
	},
}

var RevertCommand = cli.Command{
	Name:  "revert",
	Usage: "revert conversion steps",
	Description: `'reverts' attempts to revert changes done to ipfs repo by 'convert'.
It's possible to run revert when conversion failed in middle of the process or
if it was run with --keep option enabled.

Note that in some cases revert may fail in a non-graceful way. When running
revert after other programs used the datastore (like ipfs daemon), changes made
by it between 'convert' and 'revert' may be lost. This may lead to repo
corruption in extreme cases.

Use this command with care, make sure you have some free disk space.
If you have any important data in the repo it's highly recommended to backup the
repo before running this command if you haven't already.

IPFS_PATH environmental variable is respected
	`,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "force",
			Usage: "revert even if last conversion was successful",
		},
		cli.BoolFlag{
			Name:  "fix-config",
			Usage: "revert repo config from datastore_spec",
		},
	},
	Action: func(c *cli.Context) error {
		baseDir, err := getBaseDir()
		if err != nil {
			convert.Log.Fatal(err)
		}

		err = revert.Revert(baseDir, c.Bool("force"), c.Bool("fix-config"), false)
		if err != nil {
			convert.Log.Fatal(err)
		}
		return err
	},
}

var CleanupCommand = cli.Command{
	Name:  "cleanup",
	Usage: "remove leftover backup files",
	Description: `'cleanup' removes backup files left after successful convert --keep
was run.

IPFS_PATH environmental variable is respected
	`,
	Flags: []cli.Flag{},
	Action: func(c *cli.Context) error {
		baseDir, err := getBaseDir()
		if err != nil {
			convert.Log.Fatal(err)
		}

		err = revert.Revert(baseDir, c.Bool("force"), false, true)
		if err != nil {
			convert.Log.Fatal(err)
		}
		return err
	},
}

//TODO: Patch config util command

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
