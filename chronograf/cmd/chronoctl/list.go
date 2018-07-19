package main

import (
	"context"
)

type ListCommand struct {
	BoltPath string `short:"b" long:"bolt-path" description:"Full path to boltDB file (e.g. './chronograf-v1.db')" env:"BOLT_PATH" default:"chronograf-v1.db"`
}

var listCommand ListCommand

func (l *ListCommand) Execute(args []string) error {
	c, err := NewBoltClient(l.BoltPath)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx := context.Background()
	users, err := c.UsersStore.All(ctx)
	if err != nil {
		return err
	}

	w := NewTabWriter()
	WriteHeaders(w)
	for _, user := range users {
		WriteUser(w, &user)
	}
	w.Flush()

	return nil
}

func init() {
	parser.AddCommand("list-users",
		"Lists users",
		"The list-users command will list all users in the chronograf boltdb instance",
		&listCommand)
}
