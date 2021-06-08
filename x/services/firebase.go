package services

import (
	"context"
	firebase "firebase.google.com/go"
	"firebase.google.com/go/auth"
	"firebase.google.com/go/messaging"
	log "github.com/shyyawn/go-to/x/logging"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
	"google.golang.org/api/option"
)

type Firebase struct {
	ProjectId       string `mapstructure:"project_id"`
	CredentialFile  string `mapstructure:"credential_file"`
	app             *firebase.App
	messagingClient *messaging.Client
	authClient      *auth.Client
	Cancel          *context.CancelFunc
	Ctx             *context.Context
}

func (svc *Firebase) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, svc)
}

func (svc *Firebase) App() *firebase.App {
	if svc.app != nil {
		return svc.app
	}
	ctx, cancel := context.WithCancel(context.Background())
	app, err := firebase.NewApp(ctx, &firebase.Config{
		ProjectID: svc.ProjectId,
	}, option.WithCredentialsFile(svc.CredentialFile))

	if err != nil {
		cancel()
		log.Fatal(500, "Error initialization app", err)
	}
	// Set for later reference
	svc.Ctx = &ctx
	svc.Cancel = &cancel
	svc.app = app

	return svc.app
}

func (svc *Firebase) Messaging() *messaging.Client {
	if svc.messagingClient != nil {
		return svc.messagingClient
	}
	client, err := svc.App().Messaging(*svc.Ctx)
	if err != nil {
		(*svc.Cancel)()
		log.Fatal(500, "Error getting Messaging client", err)
	}
	return client
}

func (svc *Firebase) Auth() *auth.Client {
	if svc.authClient != nil {
		return svc.authClient
	}
	client, err := svc.App().Auth(*svc.Ctx)
	if err != nil {
		(*svc.Cancel)()
		log.Fatal(500, "Error getting Messaging client", err)
	}
	return client
}
