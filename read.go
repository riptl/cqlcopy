package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	cql "github.com/gocql/gocql"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	"github.com/terorie/go-quotecsv"
)

var readCmd = cobra.Command{
	Use:   "read <table> <cols...>",
	Short: "Dump data from Cassandra",
	Long: "Reads data from a cluster and exports it to CSV.\n" +
		"The first column must be the primary key.",
	Args: cobra.MinimumNArgs(2),
	Run:  runReadCmd,
}

func init() {
	f := readCmd.Flags()
	f.Bool(FlagHeader, true, "Create first row with column names")
	f.IntP(FlagPageSize, "s", 128, "Query page size")
	f.IntP(FlagPartitionKeySize, "p", 1, "Number of columns to use as partition key")
	f.Int64P(FlagOffset, "o", -9223372036854775808, "token() offset (exclusive)")
	f.Float64(FlagPrefetch, 1024, "Prefetch size")
}

type readCtx struct {
	session          *cql.Session
	tableName        string
	cols             []string
	partitionKeySize int
	offset           int64
	prefetch         float64
	pageSize         int
	writeHeader      bool
}

func runReadCmd(c *cobra.Command, args []string) {
	start := time.Now()

	// Context
	var ctx readCtx

	// Cassandra session
	var err error
	ctx.session, err = clusterConfig.CreateSession()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to connect:", err)
		os.Exit(1)
	}
	defer ctx.session.Close()

	// Check names
	ctx.tableName = args[0]
	ctx.cols = args[1:]
	if !checkIdentifier(ctx.tableName) {
		fmt.Fprintln(os.Stderr, "Invalid table name")
		os.Exit(1)
	}
	for _, col := range ctx.cols {
		if !checkIdentifier(col) {
			fmt.Fprintln(os.Stderr, "Invalid column name:", col)
			os.Exit(1)
		}
	}

	flags := c.Flags()
	ctx.partitionKeySize, err = flags.GetInt(FlagPartitionKeySize)
	if err != nil {
		panic(err)
	}
	if ctx.partitionKeySize > len(ctx.cols) {
		fmt.Fprintln(os.Stderr, "Partition key size larger than number of columns")
		os.Exit(1)
	}
	ctx.offset, err = flags.GetInt64(FlagOffset)
	if err != nil {
		panic(err)
	}
	ctx.prefetch, err = flags.GetFloat64(FlagPrefetch)
	if err != nil {
		panic(err)
	}
	ctx.pageSize, err = flags.GetInt(FlagPageSize)
	if err != nil {
		panic(err)
	}
	ctx.writeHeader, err = flags.GetBool(FlagHeader)
	if err != nil {
		panic(err)
	}

	for {
		var ok bool
		ctx.offset, ok = ctx.execute()
		ctx.writeHeader = false
		if ok {
			break
		}
	}

	log.Printf("Finished after %s", time.Since(start).String())
}

func (r *readCtx) execute() (token int64, ok bool) {
	stmt := fmt.Sprintf("SELECT token(%[1]s), %[2]s "+
		"FROM %[3]s "+
		"WHERE token(%[1]s) > %[4]d;",
		strings.Join(r.cols[:r.partitionKeySize], ", "),
		strings.Join(r.cols, ", "),
		r.tableName,
		r.offset)
	log.Print("Using statement ", stmt)
	query := r.session.Query(stmt)
	query.RetryPolicy(&cql.ExponentialBackoffRetryPolicy{
		NumRetries: 99999,
		Min:        100 * time.Millisecond,
		Max:        10 * time.Minute,
	})
	query.Prefetch(r.prefetch)
	query.PageSize(r.pageSize)
	query.Idempotent(true)
	iter := query.Iter()
	defer func() {
		if err := iter.Close(); err != nil {
			ok = false
			log.Printf("Scan aborted, retrying: %s", err)
		} else {
			ok = true
		}
	}()

	out := csv.NewWriter(os.Stdout)
	defer out.Flush()

	page := 0

	// Get column info
	row := make([]interface{}, len(r.cols)+1)
	rowNames := make([]csv.Column, len(r.cols))
	for i, col := range iter.Columns() {
		row[i] = col.TypeInfo.New()
		if i > 0 {
			rowNames[i-1] = csv.Column{
				Quoted: true,
				Value:  col.Name,
			}
		}
	}

	// Write CSV header
	if r.writeHeader {
		_ = out.Write(rowNames)
	}

	outRow := make([]csv.Column, len(r.cols))
	rowCount := 0
	token = r.offset
	for {
		switchPage := false
		if iter.WillSwitchPage() {
			switchPage = true
		}
		if ok := iter.Scan(row...); !ok {
			break
		}
		token = *(row[0].(*int64))
		if switchPage {
			page++
			rowCount += iter.NumRows()
			log.Printf("%8d page |\t%10d done |\t token=% 20d",
				page, rowCount, token)
		}
		for i := range outRow {
			outRow[i].Value = cast.ToString(row[i+1])
			if _, ok := row[i+1].(*string); ok {
				outRow[i].Quoted = true
			}
		}
		if err := out.Write(outRow); err != nil {
			fmt.Fprintf(os.Stderr, "CSV output failed: %s\n", err)
			os.Exit(1)
		}
	}

	return
}
