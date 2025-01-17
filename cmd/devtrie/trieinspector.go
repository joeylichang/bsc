package main

import (
	"fmt"
	"time"
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

    if lastheader {
    	headerHash := rawdb.ReadHeadHeaderHash(db)
    	num = *(rawdb.ReadHeaderNumber(db, headerHash))
    } 

    hash := rawdb.ReadCanonicalHash(db, num)
    if hash == (common.Hash{}) {
		fmt.Println("the num of header is empty")
		return nil
			
	}
    header := rawdb.ReadHeader(db, hash, num)

    tr, err := trie.New(header.Root, trie.NewDatabase(db))
    if err != nil {
    	return err
    }

    inspector, err := trie.NewInspector(tr, num)
    if err != nil {
    	return err
    }

    startTime:=time.Now().Format("2006-01-02 15:04:05") 
    err = inspector.Do()
    endTime:=time.Now().Format("2006-01-02 15:04:05") 
    fmt.Println("start time: " + startTime + " , end time: " + endTime)
    return err
}