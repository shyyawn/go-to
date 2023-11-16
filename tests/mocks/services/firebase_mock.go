package services

import (
	firebase "firebase.google.com/go"
	"firebase.google.com/go/auth"
	"firebase.google.com/go/messaging"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
)

type FirebaseMock struct {
	mock.Mock
}

func (svc *FirebaseMock) LoadFromConfig(key string, config *viper.Viper) error {
	args := svc.Called(key, config)
	return args.Error(0)
}

func (svc *FirebaseMock) App() *firebase.App {
	args := svc.Called()
	return args.Get(0).(*firebase.App)
}

func (svc *FirebaseMock) Messaging() *messaging.Client {
	args := svc.Called()
	return args.Get(0).(*messaging.Client)
}

func (svc *FirebaseMock) Auth() *auth.Client {
	args := svc.Called()
	return args.Get(0).(*auth.Client)
}
