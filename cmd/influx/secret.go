package main

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
	input "github.com/tcnksm/go-input"
)

type secretSVCsFn func() (influxdb.SecretService, influxdb.OrganizationService, func(*input.UI) string, error)

func cmdSecret(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	builder := newCmdSecretBuilder(newSecretSVCs, opt)
	builder.globalFlags = f
	return builder.cmd()
}

type cmdSecretBuilder struct {
	genericCLIOpts
	*globalFlags

	svcFn secretSVCsFn

	key string
	org organization
}

func newCmdSecretBuilder(svcsFn secretSVCsFn, opt genericCLIOpts) *cmdSecretBuilder {
	return &cmdSecretBuilder{
		genericCLIOpts: opt,
		svcFn:          svcsFn,
	}
}

func (b *cmdSecretBuilder) cmd() *cobra.Command {
	cmd := b.newCmd("secret", nil)
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
	cmd.MarkFlagRequired("key")
	b.org.register(cmd, false)

	return cmd
}

func (b *cmdSecretBuilder) cmdDelete() *cobra.Command {
	cmd := b.newCmd("delete", b.cmdDeleteRunEFn)
	cmd.Short = "Delete secret"

	cmd.Flags().StringVarP(&b.key, "key", "k", "", "The secret key (required)")
	cmd.MarkFlagRequired("key")
	b.org.register(cmd, false)

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
	secret := getSecretFn(ui)

	if err := scrSVC.PatchSecrets(ctx, orgID, map[string]string{b.key: secret}); err != nil {
		return fmt.Errorf("failed to update secret with key %q: %v", b.key, err)
	}

	w := b.newTabWriter()
	w.WriteHeaders("Key", "OrgID", "Updated")
	w.Write(map[string]interface{}{
		"Key":     b.key,
		"OrgID":   orgID,
		"Updated": true,
	})
	w.Flush()

	return nil
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

	w := b.newTabWriter()
	w.WriteHeaders("Key", "OrgID", "Deleted")
	w.Write(map[string]interface{}{
		"Key":     b.key,
		"OrgID":   orgID,
		"Deleted": true,
	})
	w.Flush()

	return nil
}

func (b *cmdSecretBuilder) cmdFind() *cobra.Command {
	cmd := b.newCmd("find", b.cmdFindRunEFn)
	cmd.Short = "Find secrets"
	b.org.register(cmd, false)

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

	secrets, err := scrSVC.GetSecretKeys(context.Background(), orgID)
	if err != nil {
		return fmt.Errorf("failed to retrieve secret keys: %s", err)
	}

	w := b.newTabWriter()
	w.WriteHeaders("Key", "OrganizationID")
	for _, s := range secrets {
		w.Write(map[string]interface{}{
			"Key":            s,
			"OrganizationID": orgID,
		})
	}
	w.Flush()

	return nil
}

func newSecretSVCs() (influxdb.SecretService, influxdb.OrganizationService, func(*input.UI) string, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, nil, nil, err
	}
	orgSvc := &http.OrganizationService{Client: httpClient}

	return &http.SecretService{Client: httpClient}, orgSvc, getSecret, nil
}
