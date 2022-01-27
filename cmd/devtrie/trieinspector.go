package main

import (
	"fmt"
	"path/filepath"
	"gopkg.in/urfave/cli.v1"

	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)


var trieInspectorCommand = cli.Command{
	Name:   "inspector",
	Usage:  "statistics triedb node info",
	Action: trieInspectorRun,
	Flags: []cli.Flag{
		cli.StringFlag{Name: "path"},
		cli.Uint64Flag{Name: "num"},
		cli.BoolFlag{Name: "lastheader"},
	},
}

func trieInspectorRun(ctx *cli.Context) error {
	path 		:= ctx.String("path")
	num  		:= ctx.Uint64("num")
	lastheader  := ctx.Bool("lastheader")


    db, err := rawdb.NewLevelDBDatabaseWithFreezer(path, 512, 20000, filepath.Join(path, "ancient"), "eth/db/chaindata/", true)
    if err != nil {
    	return err
    }

    var rootHash common.Hash
    if lastheader {
    	rootHash = rawdb.ReadHeadHeaderHash(db)
    } else {
    	hash := rawdb.ReadCanonicalHash(db, num)
		if hash == (common.Hash{}) {
			fmt.Println("the num of header is empty")
			return nil
			
		}
		header := rawdb.ReadHeader(db, hash, num)
		rootHash = header.Root
    }

    tr, err := trie.New(rootHash, trie.NewDatabase(db))
    if err != nil {
    	return err
    }

    inspector, err := trie.NewInspector(tr, num)
    if err != nil {
    	return err
    }

    return inspector.Do()
}