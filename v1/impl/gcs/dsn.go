package gcs

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/bww/go-gcputil/auth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

var ErrCredentialNotFound = errors.New("Credential not found")

type DSN struct {
	ProjectId string
	Prefix    string
	Options   []option.ClientOption
}

func ParseDSN(dsn string) (DSN, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return DSN{}, err
	}

	var prefix string
	if len(u.Path) > 0 {
		p := u.Path
		if p[0] == '/' {
			p = p[1:]
		}
		if x := strings.Index(p, "/"); x > 0 {
			prefix = p[x+1:]
		} else {
			prefix = p
		}
	}

	var opts []option.ClientOption
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		if token := os.Getenv("STORAGE_ACCESS_TOKEN"); token != "" {
			creds := &google.Credentials{
				TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
					AccessToken: token,
					TokenType:   "Bearer",
				}),
			}
			opts = append(opts, option.WithCredentials(creds))
		} else {
			creds, _, err := auth.Credentials(dsn, storage.ScopeReadWrite)
			if err != nil {
				return DSN{}, fmt.Errorf("%w: %v", ErrCredentialNotFound, err)
			}
			opts = append(opts, option.WithCredentials(creds))
		}
	}

	return DSN{
		ProjectId: u.Host,
		Prefix:    prefix,
		Options:   opts,
	}, nil
}
