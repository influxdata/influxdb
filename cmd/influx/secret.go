package main

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/internal"
	isecret "github.com/influxdata/influxdb/v2/secret"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
	"github.com/tcnksm/go-input"
)

type secretSVCsFn func() (influxdb.SecretService, influxdb.OrganizationService, func(*input.UI) string, error)

func cmdSecret(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	builder := newCmdSecretBuilder(newSecretSVCs, f, opt)
	return builder.cmd()
}

type cmdSecretBuilder struct {
	genericCLIOpts
	*globalFlags

	svcFn secretSVCsFn

	json        bool
	hideHeaders bool
	key         string
	value       string
	org         organization
}

func newCmdSecretBuilder(svcsFn secretSVCsFn, f *globalFlags, opt genericCLIOpts) *cmdSecretBuilder {
	return &cmdSecretBuilder{
		genericCLIOpts: opt,
		globalFlags:    f,
		svcFn:          svcsFn,
	}
}

func (b *cmdSecretBuilder) cmd() *cobra.Command {
	cmd := b.genericCLIOpts.newCmd("secret", nil, false)
	cmd.Short = "Secret management commands"
	cmd.Run = seeHelp
	cmd.AddCommand(
		b.cmdDelete(),
		b.cmdFind(),
		b.cmdUpdate(),
	)
	return cmd
}

func (b *cmdSecretBuilder) cmdUpdate() *cobra.Command {
	cmd := b.newCmd("update", b.cmdUpdateRunEFn)
	cmd.Short = "Update secret"

	cmd.Flags().StringVarP(&b.key, "key", "k", "", "The secret key (required)")
	cmd.Flags().StringVarP(&b.value, "value", "v", "", "Optional secret value for scripting convenience, using this might expose the secret to your local history")
	cmd.MarkFlagRequired("key")
	b.org.register(b.viper, cmd, false)
	b.registerPrintFlags(cmd)

	return cmd
}

func (b *cmdSecretBuilder) cmdUpdateRunEFn(cmd *cobra.Command, args []string) error {
	scrSVC, orgSVC, getSecretFn, err := b.svcFn()
	if err != nil {
		return err
	}
	orgID, err := b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	ctx := context.Background()

	ui := &input.UI{
		Writer: b.genericCLIOpts.w,
		Reader: b.genericCLIOpts.in,
	}
	var secretVal string
	if b.value != "" {
		secretVal = b.value
	} else {
		secretVal = getSecretFn(ui)
	}

	err = scrSVC.PatchSecrets(ctx, orgID, map[string]string{
		b.key: secretVal,
	})
	if err != nil {
		return fmt.Errorf("failed to update secret with key %q: %v", b.key, err)
	}

	return b.printSecrets(secretPrintOpt{
		secret: secret{
			key:   b.key,
			orgID: orgID,
		},
	})
}

func (b *cmdSecretBuilder) cmdDelete() *cobra.Command {
	cmd := b.newCmd("delete", b.cmdDeleteRunEFn)
	cmd.Short = "Delete secret"

	cmd.Flags().StringVarP(&b.key, "key", "k", "", "The secret key (required)")
	cmd.MarkFlagRequired("key")
	b.org.register(b.viper, cmd, false)
	b.registerPrintFlags(cmd)

	return cmd
}

func (b *cmdSecretBuilder) cmdDeleteRunEFn(cmd *cobra.Command, args []string) error {
	scrSVC, orgSVC, _, err := b.svcFn()
	if err != nil {
		return err
	}
	orgID, err := b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if err := scrSVC.DeleteSecret(ctx, orgID, b.key); err != nil {
		return fmt.Errorf("failed to delete secret with key %q: %v", b.key, err)
	}

	return b.printSecrets(secretPrintOpt{
		deleted: true,
		secret: secret{
			key:   b.key,
			orgID: orgID,
		},
	})
}

func (b *cmdSecretBuilder) cmdFind() *cobra.Command {
	cmd := b.newCmd("list", b.cmdFindRunEFn)
	cmd.Short = "List secrets"
	cmd.Aliases = []string{"find", "ls"}

	b.org.register(b.viper, cmd, false)
	b.registerPrintFlags(cmd)

	return cmd
}

func (b *cmdSecretBuilder) cmdFindRunEFn(cmd *cobra.Command, args []string) error {
	scrSVC, orgSVC, _, err := b.svcFn()
	if err != nil {
		return err
	}

	orgID, err := b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	platformSecrets, err := scrSVC.GetSecretKeys(context.Background(), orgID)
	if err != nil {
		return fmt.Errorf("failed to retrieve secret keys: %s", err)
	}

	secrets := make([]secret, 0, len(platformSecrets))
	for _, key := range platformSecrets {
		secrets = append(secrets, secret{
			key:   key,
			orgID: orgID,
		})
	}

	return b.printSecrets(secretPrintOpt{
		secrets: secrets,
	})
}

func (b *cmdSecretBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := b.genericCLIOpts.newCmd(use, runE, true)
	b.globalFlags.registerFlags(b.viper, cmd)
	return cmd
}

func (b *cmdSecretBuilder) registerPrintFlags(cmd *cobra.Command) {
	registerPrintOptions(b.viper, cmd, &b.hideHeaders, &b.json)
}

func (b *cmdSecretBuilder) printSecrets(opt secretPrintOpt) error {
	if b.json {
		var v interface{} = opt.secrets
		if opt.secrets == nil {
			v = opt.secret
		}
		return b.writeJSON(v)
	}

	w := b.newTabWriter()
	defer w.Flush()

	w.HideHeaders(b.hideHeaders)

	headers := []string{"Key", "Organization ID"}
	if opt.deleted {
		headers = append(headers, "Deleted")
	}
	w.WriteHeaders(headers...)

	if opt.secrets == nil {
		opt.secrets = append(opt.secrets, opt.secret)
	}

	for _, s := range opt.secrets {
		m := map[string]interface{}{
			"Key":             s.key,
			"Organization ID": s.orgID.String(),
		}
		if opt.deleted {
			m["Deleted"] = true
		}
		w.Write(m)
	}

	return nil
}

type (
	secretPrintOpt struct {
		deleted bool
		secret  secret
		secrets []secret
	}

	secret struct {
		key   string
		orgID influxdb.ID
	}
)

func newSecretSVCs() (influxdb.SecretService, influxdb.OrganizationService, func(*input.UI) string, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, nil, nil, err
	}

	orgSvc := &tenant.OrgClientService{Client: httpClient}

	return &isecret.Client{Client: httpClient}, orgSvc, internal.GetSecret, nil
}
