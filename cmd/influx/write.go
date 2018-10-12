package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/http"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var writeCmd = &cobra.Command{
	Use:   "write [line protocol or @/path/to/points.txt",
	Short: "Write points to influxdb",
	Long: `Write a single line of line protocol to influx db,
		or add an entire file specified with an @ prefix`,
	Args: cobra.ExactArgs(1),
	Run:  fluxWriteF,
}

var writeFlags struct {
	OrgID    string
	Org      string
	BucketID string
	Bucket   string
}

func init() {
	writeCmd.PersistentFlags().StringVar(&writeFlags.OrgID, "org-id", "", "id of the organization that owns the bucket")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		writeFlags.OrgID = h
	}

	writeCmd.PersistentFlags().StringVarP(&writeFlags.Org, "org", "o", "", "name of the organization that owns the bucket")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		writeFlags.Org = h
	}

	writeCmd.PersistentFlags().StringVar(&writeFlags.BucketID, "bucket-id", "", "ID of destination bucket")
	viper.BindEnv("BUCKET_ID")
	if h := viper.GetString("BUCKET_ID"); h != "" {
		writeFlags.BucketID = h
	}

	writeCmd.PersistentFlags().StringVarP(&writeFlags.Org, "bucket", "b", "", "name of destination bucket")
	viper.BindEnv("BUCKET_NAME")
	if h := viper.GetString("BUCKET_NAM"); h != "" {
		writeFlags.Bucket = h
	}
}

func fluxWriteF(cmd *cobra.Command, args []string) {
	ctx := context.TODO()

	if writeFlags.Org != "" && writeFlags.OrgID != "" {
		fmt.Println("must specify exactly one of org or org-id")
		cmd.Usage()
		os.Exit(1)
	}

	if writeFlags.Bucket != "" && writeFlags.BucketID != "" {
		fmt.Println("must specify exactly one of org or org-id")
		cmd.Usage()
		os.Exit(1)
	}

	bs := &http.BucketService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var err error
	filter := platform.BucketFilter{}

	if writeFlags.BucketID != "" {
		filter.ID, err = platform.IDFromString(writeFlags.BucketID)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	if writeFlags.Bucket != "" {
		filter.Name = &writeFlags.Bucket
	}

	if writeFlags.OrgID != "" {
		filter.OrganizationID, err = platform.IDFromString(writeFlags.OrgID)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	if writeFlags.Org != "" {
		filter.Organization = &writeFlags.Org
	}

	buckets, _, err := bs.FindBuckets(ctx, filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	bucketID, orgID := buckets[0].ID, buckets[0].OrganizationID

	var r io.Reader
	if args[0] == "-" {
		r = os.Stdin
	} else if len(args[0]) > 0 && args[0][0] == '@' {
		f, err := os.Open(args[0][1:])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		r = f
	} else {
		r = strings.NewReader(args[0])
	}

	s := &http.WriteService{
		Addr:  flags.host,
		Token: flags.token,
	}

	if err = s.Write(ctx, orgID, bucketID, r); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
