package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cql "github.com/gocql/gocql"
	"github.com/spf13/cobra"
	"github.com/terorie/go-quotecsv"
)

var writeCmd = cobra.Command{
	Use:   "write <table> <cols...>",
	Short: "Copy from CSV to Cassandra",
	Long:  "Streams CSV from standard input and writes it to a Cassandra cluster.",
	Args:  cobra.MinimumNArgs(2),
	Run:   runWriteCmd,
}

const (
	FlagBoolStyle       = "bool-style"
	FlagTimeFormat      = "time-format"
	FlagHeader          = "header"
	FlagMaxAttempts     = "max-attempts"
	FlagNull            = "null"
	FlagNumProcesses    = "num-processes"
	FlagReportFrequency = "report-frequency"
	FlagChunkSize       = "chunk-size"
	FlagMaxBatchSize    = "max-batch-size"
	FlagMaxInsertErrors = "max-insert-errors"
	FlagMaxParseErrors  = "max-parse-errors"
	FlagBackoff         = "backoff"
)

func init() {
	f := writeCmd.Flags()
	f.String(FlagBoolStyle, "true,false", "Boolean indicators for true and false (case-insensitive)")
	f.StringVar(&timeFormat, FlagTimeFormat, "2006-01-02 15:04:05-0700", "Timestamp format")
	f.Bool(FlagHeader, false, "First row contains column names")
	f.UintVar(&maxAttempts, FlagMaxAttempts, 5, "Max attempts (Zero means infinite)")
	f.StringVar(&nullValue, FlagNull, "NULL", "Special unquoted literal marking a null value")
	f.Uint(FlagNumProcesses, 8, "Number of worker processes (goroutines)")
	f.DurationVar(&reportInterval, FlagReportFrequency, 250*time.Millisecond, "Frequency with which status is displayed")
	f.UintVar(&chunkSize, FlagChunkSize, 1000, "Number of items in an import batch")
	f.Int64Var(&maxBatchSize, FlagMaxBatchSize, 20, "Maximum size of an import batch in kB")
	f.Int32Var(&maxInsertErrors, FlagMaxInsertErrors, -1, "Maximum number of batch insert errors (Negative value: no maximum)")
	f.Int(FlagMaxParseErrors, -1, "Maximum number of parsing errors (Negative value: no maximum)")
	f.DurationVar(&backoff, FlagBackoff, time.Second, "Duration to wait after an error")
}

var (
	maxAttempts     uint
	maxInsertErrors int32
	chunkSize       uint
	maxBatchSize    int64
	nullValue       string
	reportInterval  time.Duration
	backoff         time.Duration
	timeFormat      string
)

type writeCtx struct {
	ctx        context.Context
	cancel     context.CancelFunc
	table      string
	cols       []string
	stmt       string
	session    *cql.Session
	trueValue  string
	falseValue string
	errCount   int32
	inserted   int64
	start      time.Time
	wg         sync.WaitGroup
}

func runWriteCmd(c *cobra.Command, args []string) {
	// Context
	var ctx writeCtx
	ctx.ctx, ctx.cancel = context.WithCancel(context.Background())
	ctx.table = args[0]
	ctx.cols = args[1:]
	ctx.start = time.Now()
	flags := c.Flags()

	// Prepare statement
	if !checkIdentifier(ctx.table) {
		fmt.Fprintln(os.Stderr, "Invalid table name")
		os.Exit(1)
	}
	for _, col := range ctx.cols {
		if !checkIdentifier(col) {
			fmt.Fprintln(os.Stderr, "Invalid column name:", col)
			os.Exit(1)
		}
	}
	ctx.stmt = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		ctx.table,
		strings.Join(ctx.cols, ", "),
		"?"+strings.Repeat(", ?", len(ctx.cols)-1))

	// Cassandra session
	var err error
	ctx.session, err = clusterConfig.CreateSession()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to connect:", err)
		os.Exit(1)
	}
	defer ctx.session.Close()

	// Settings
	maxParseErrors, _ := flags.GetInt(FlagMaxParseErrors)
	procs, _ := flags.GetUint(FlagNumProcesses)
	if procs == 0 {
		fmt.Fprintln(os.Stderr, FlagNumProcesses+"must be greater than zero")
		os.Exit(1)
	}
	boolValsStr, _ := flags.GetString(FlagBoolStyle)
	boolVals := strings.SplitN(boolValsStr, ",", 2)
	if len(boolVals) != 2 {
		fmt.Fprintln(os.Stderr, "Invalid "+FlagBoolStyle)
		os.Exit(1)
	}
	ctx.trueValue = boolVals[0]
	ctx.falseValue = boolVals[1]

	// Read state
	rd := csv.NewReader(os.Stdin)
	rows := make(chan []csv.Column)
	parseErrors := 0

	// Writers
	if reportInterval < 100*time.Millisecond {
		reportInterval = 100 * time.Millisecond
	}
	go ctx.reporter(reportInterval)
	for i := uint(0); i < procs; i++ {
		go ctx.write(rows)
	}

	// Reader
	if hdr, _ := flags.GetBool(FlagHeader); hdr {
		_, _ = rd.Read()
	}
	for {
		row, err := rd.Read()
		if parseErr, ok := err.(*csv.ParseError); ok {
			// CSV line parse error
			log.Print(parseErr)
			parseErrors++
			if maxParseErrors >= 0 && parseErrors > maxParseErrors {
				log.Printf("Giving up after %d parse errors", parseErrors)
				break
			}
		} else if err == io.EOF {
			break
		} else if err != nil {
			// I/O error
			log.Print(err)
			break
		}
		rows <- row
	}

	// Wait for writers to exit
	ctx.wg.Wait()
}

func (w *writeCtx) reporter(interval time.Duration) {
	var lastCount int64
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-time.Tick(interval):
			total := atomic.LoadInt64(&w.inserted)
			errors := atomic.LoadInt32(&w.errCount)
			delta := total - lastCount
			perSecond := float64(delta) / interval.Seconds()
			average := float64(total) / time.Since(w.start).Seconds()
			log.Printf("%10d done |\t%6d fail |\t%6.0f/s cur |\t%6.0f/s avg",
				total, errors, perSecond, average)
			lastCount = total
		}
	}
}

func (w *writeCtx) write(rows <-chan []csv.Column) {
	defer w.wg.Done()
	b := writeBatcher{
		writeCtx: w,
	}
	for {
		select {
		case <-w.ctx.Done():
			break
		case row, ok := <-rows:
			if !ok {
				break
			}
			b.next(row)
		}
	}
}

func (w *writeCtx) parseValues(ss []csv.Column) (vals []interface{}) {
	vals = make([]interface{}, len(ss))
	for i, s := range ss {
		vals[i] = w.parseValue(s)
	}
	return
}

func (w *writeCtx) parseValue(s csv.Column) interface{} {
	if s.Quoted {
		return s.Value
	} else if s.Value == w.falseValue {
		return false
	} else if s.Value == w.trueValue {
		return true
	} else if s.Value == nullValue {
		return nil
	} else if t, err := time.Parse(timeFormat, s.Value); err == nil {
		return t
	} else if x, err := strconv.ParseInt(s.Value, 10, 64); err == nil {
		return x
	} else {
		return s.Value
	}
}

type writeBatcher struct {
	*writeCtx
	batch     *cql.Batch
	batchSize int64
}

func (b *writeBatcher) next(row []csv.Column) {
	if b.batch == nil {
		// Create new batch
		b.batch = b.session.NewBatch(cql.UnloggedBatch)
		b.batchSize = 0
	}
	b.batch.Query(b.stmt, b.parseValues(row)...)
	for _, col := range row {
		b.batchSize += int64(len(col.Value))
	}
	if uint(len(b.batch.Entries)) >= chunkSize || b.batchSize/1000 >= maxBatchSize {
		// Commit batch
		if !b.commit() {
			totalErrs := atomic.AddInt32(&b.errCount, 1)
			if totalErrs > maxInsertErrors {
				log.Printf("Aborting after %d insert errors", totalErrs)
			}
		}
	}
}

func (b *writeBatcher) commit() bool {
	var err error
	for i := uint(0); i < maxAttempts; i++ {
		if err = b.session.ExecuteBatch(b.batch); err == nil {
			atomic.AddInt64(&b.inserted, int64(len(b.batch.Entries)))
			return true
		}
		time.Sleep(backoff)
	}
	log.Printf("Giving up after %d failed insert attempts: %s", maxAttempts, err)
	return false
}
