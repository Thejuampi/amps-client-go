package gofercli

import (
	"fmt"
	"strings"

	"github.com/Thejuampi/amps-client-go/amps"
	kerberosauth "github.com/Thejuampi/amps-client-go/amps/auth/kerberos"
)

type authenticatorFactory func() (amps.Authenticator, error)

var authenticatorFactories = map[string]authenticatorFactory{
	"": func() (amps.Authenticator, error) {
		return nil, nil
	},
	"default": func() (amps.Authenticator, error) {
		return nil, nil
	},
	"defaultauthenticatorfactory": func() (amps.Authenticator, error) {
		return nil, nil
	},
	"com.crankuptheamps.spark.defaultauthenticatorfactory": func() (amps.Authenticator, error) {
		return nil, nil
	},
	"kerberos": func() (amps.Authenticator, error) {
		return kerberosauth.NewAuthenticator(kerberosauth.Config{}), nil
	},
	"kerberosauthenticatorfactory": func() (amps.Authenticator, error) {
		return kerberosauth.NewAuthenticator(kerberosauth.Config{}), nil
	},
	"com.crankuptheamps.spark.kerberosauthenticatorfactory": func() (amps.Authenticator, error) {
		return kerberosauth.NewAuthenticator(kerberosauth.Config{}), nil
	},
}

func resolveAuthenticator(name string) (amps.Authenticator, error) {
	var normalized = strings.TrimSpace(strings.ToLower(name))
	if factory, ok := authenticatorFactories[normalized]; ok {
		return factory()
	}
	return nil, fmt.Errorf("unsupported authenticator %q", name)
}
