package main

import (
	"io"

	"github.com/spf13/cobra"
)

func completionCmd(rootCmd *cobra.Command) *cobra.Command {
	writeZSH := func(w io.Writer) error {
		if err := rootCmd.GenZshCompletion(w); err != nil {
			return err
		}
		_, err := io.WriteString(w, "\ncompdef _influx influx\n")
		return err
	}

	return &cobra.Command{
		Use:       "completion [bash|zsh]",
		Short:     "Generates completion scripts",
		Args:      cobra.ExactValidArgs(1),
		ValidArgs: []string{"bash", "zsh", "powershell"},
		Long: `
	Outputs shell completion for the given shell (bash or zsh)

	OS X:
		$ source $(brew --prefix)/etc/bash_completion	# for bash users
		$ source <(influx completion bash)		# for bash users
		$ source <(influx completion zsh)		# for zsh users

	Ubuntu:
		$ source /etc/bash-completion	   # for bash users
		$ source <(influx completion bash) # for bash users
		$ source <(influx completion zsh)  # for zsh users

	Additionally, you may want to add this to your .bashrc/.zshrc
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			writer := rootCmd.OutOrStdout()
			switch args[0] {
			case "bash":
				return rootCmd.GenBashCompletion(writer)
			case "powershell":
				return rootCmd.GenPowerShellCompletion(writer)
			case "zsh":
				return writeZSH(writer)
			}
			return nil
		},
	}
}
