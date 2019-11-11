package main

import (
	"fmt"
	"os"

	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
)

var app = cobra.Command{
	Use:   "cqlcopy",
	Short: "Efficient replacement for cqlsh COPY",
	Long: "cqlcopy is a performant replacement for cqlsh's COPY functionality.\n" +
		"Used for transferring bulk data from/to Apache Cassandra.\n" +
		"Compatible with RFC 4180 CSV files and written in Go.\n" +
		"Repository: https://github.com/terorie/cqlcopy",
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		f := cmd.Flags()
		hosts, _ := f.GetStringSlice(FlagConnect)
		clusterConfig = gocql.NewCluster(hosts...)
		clusterConfig.Keyspace, _ = f.GetString(FlagKeyspace)
	},
}

var clusterConfig *gocql.ClusterConfig

const (
	FlagConnect  = "connect"
	FlagKeyspace = "keyspace"
)

func init() {
	app.AddCommand(
		&writeCmd,
		&readCmd,
		//&ssTableCmd,
	)
	app.AddCommand()
	p := app.PersistentFlags()
	p.StringSliceP(FlagConnect, "c", []string{"localhost:9042"}, "Node CQL addresses")
	p.StringP(FlagKeyspace, "k", "", "Default keyspace")
}

func main() {
	if err := app.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
