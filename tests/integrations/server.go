package integrations

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
)

type Server struct {
	p          *os.Process
	configFile string
	sslOnly    bool
	args       []string
}

func NewServer() (*Server, error) {
	return &Server{}, nil
}

func (s *Server) Start() error {
	if s.p != nil {
		return fmt.Errorf("Server is already running with pid %d", s.p.Pid)
	}

	log.Printf("Starting server\n")
	dir, err := os.Getwd()
	if err != nil {
		return err
	}

	log.Println(dir)
	root := filepath.Join(dir, "..")
	filename := filepath.Join(root, "influxdb")
	if runtime.GOOS == "windows" {
		filename += ".exe"
	}
	if s.configFile == "" {
		s.configFile = "../server_single_test.toml"
	}
	args := []string{filename, "-config", s.configFile}
	args = append(args, s.args...)
	log.Println(filename)

	p, err := os.StartProcess(filename, args, &os.ProcAttr{
		Dir:   root,
		Env:   os.Environ(),
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	})
	if err != nil {
		return err
	}
	s.p = p
	return nil
}
