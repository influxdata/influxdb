package cli

import (
	"fmt"

	"github.com/spf13/pflag"
	"go.uber.org/zap/zapcore"
)

type levelValue zapcore.Level

func newLevelValue(val zapcore.Level, p *zapcore.Level) *levelValue {
	*p = val
	return (*levelValue)(p)
}

func (l *levelValue) String() string {
	return zapcore.Level(*l).String()
}
func (l *levelValue) Set(s string) error {
	var level zapcore.Level
	if err := level.Set(s); err != nil {
		return fmt.Errorf("unknown log level; supported levels are debug, info, warn, error")
	}
	*l = levelValue(level)
	return nil
}

func (l *levelValue) Type() string {
	return "Log-Level"
}

// LevelVar defines a zapcore.Level flag with specified name, default value, and usage string.
// The argument p points to a zapcore.Level variable in which to store the value of the flag.
func LevelVar(fs *pflag.FlagSet, p *zapcore.Level, name string, value zapcore.Level, usage string) {
	LevelVarP(fs, p, name, "", value, usage)
}

// LevelVarP is like LevelVar, but accepts a shorthand letter that can be used after a single dash.
func LevelVarP(fs *pflag.FlagSet, p *zapcore.Level, name, shorthand string, value zapcore.Level, usage string) {
	fs.VarP(newLevelValue(value, p), name, shorthand, usage)
}
