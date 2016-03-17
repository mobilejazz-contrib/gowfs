/*
gowfs is Go bindings for the Hadoop HDFS over its WebHDFS interface.
gowfs uses JSON marshalling to expose typed values from HDFS.
See https://github.com/vladimirvivien/gowfs.
*/
package gowfs

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
)
import "net"
import "net/http"
import "net/url"
import "io/ioutil"

const (
	OP_OPEN                  = "OPEN"
	OP_CREATE                = "CREATE"
	OP_APPEND                = "APPEND"
	OP_CONCAT                = "CONCAT"
	OP_RENAME                = "RENAME"
	OP_DELETE                = "DELETE"
	OP_SETPERMISSION         = "SETPERMISSION"
	OP_SETOWNER              = "SETOWNER"
	OP_SETREPLICATION        = "SETREPLICATION"
	OP_SETTIMES              = "SETTIMES"
	OP_MKDIRS                = "MKDIRS"
	OP_CREATESYMLINK         = "CREATESYMLINK"
	OP_LISTSTATUS            = "LISTSTATUS"
	OP_GETFILESTATUS         = "GETFILESTATUS"
	OP_GETCONTENTSUMMARY     = "GETCONTENTSUMMARY"
	OP_GETFILECHECKSUM       = "GETFILECHECKSUM"
	OP_GETDELEGATIONTOKEN    = "GETDELEGATIONTOKEN"
	OP_GETDELEGATIONTOKENS   = "GETDELEGATIONTOKENS"
	OP_RENEWDELEGATIONTOKEN  = "RENEWDELEGATIONTOKEN"
	OP_CANCELDELEGATIONTOKEN = "CANCELDELEGATIONTOKEN"
)

// Hack for in-lining multi-value functions
func µ(v ...interface{}) []interface{} {
	return v
}

// This type maps fields and functions to HDFS's FileSystem class.
type FileSystem struct {
	Config    Configuration
	client    http.Client
	AuthToken OAuthToken
}

type OAuthToken struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int32  `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
}

func NewFileSystem(conf Configuration) (*FileSystem, error) {
	fs := &FileSystem{
		Config: conf,
	}

	fs.client = http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				c, err := net.DialTimeout(netw, addr, conf.ConnectionTimeout)
				if err != nil {
					return nil, err
				}

				return c, nil
			},
			MaxIdleConnsPerHost: conf.MaxIdleConnsPerHost,
		},
	}

	//Get the oauth token
	url := conf.AuthServer + "/cosmos-auth/v1/token"

	payload := strings.NewReader("grant_type=password&username=" + conf.Username + "&password=" + conf.Password)

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: tr}
	req, _ := http.NewRequest("POST", url, payload)
	req.Header.Add("cache-control", "no-cache")
	req.Header.Add("content-type", "application/x-www-form-urlencoded")

	res, err := client.Do(req)
	if err != nil {
		_ = fmt.Errorf("Auth error %s", err.Error())
		return fs, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		_ = fmt.Errorf("Auth error %s", err.Error())
		return fs, err
	}

	var data OAuthToken
	json.Unmarshal(body, &data)
	fs.AuthToken = data

	return fs, nil
}

// Builds the canonical URL used for remote request
func buildRequestUrl(conf Configuration, p *Path, params *map[string]string) (*url.URL, error) {
	u, err := conf.GetNameNodeUrl()
	if err != nil {
		return nil, err
	}

	//prepare URL - add Path and "op" to URL
	if p != nil {
		if p.Name[0] == '/' {
			u.Path = u.Path + p.Name
		} else {
			u.Path = u.Path + "/" + p.Name
		}
	}

	q := u.Query()

	// attach params
	if params != nil {
		for key, val := range *params {
			q.Add(key, val)
		}
	}
	u.RawQuery = q.Encode()

	return u, nil
}

//BuildRequest builds a request
func (fs *FileSystem) BuildRequest(action string, p *Path, params *map[string]string) (*http.Request, error) {
	u, err := buildRequestUrl(fs.Config, p, params)
	if err != nil {
		return nil, err
	}

	// take over default transport to avoid redirect
	req, _ := http.NewRequest("PUT", u.String(), nil)
	req.Header.Set("X-Auth-Token", fs.AuthToken.AccessToken)

	return req, nil
}

func makeHdfsData(data []byte) (HdfsJsonData, error) {
	if len(data) == 0 || data == nil {
		return HdfsJsonData{}, nil
	}
	var jsonData HdfsJsonData
	jsonErr := json.Unmarshal(data, &jsonData)

	if jsonErr != nil {
		return HdfsJsonData{}, jsonErr
	}

	// check for remote exception
	if jsonData.RemoteException.Exception != "" {
		return HdfsJsonData{}, jsonData.RemoteException
	}

	return jsonData, nil

}

func responseToHdfsData(rsp *http.Response) (HdfsJsonData, error) {
	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return HdfsJsonData{}, err
	}
	return makeHdfsData(body)
}

func requestHdfsData(client http.Client, req http.Request) (HdfsJsonData, error) {
	rsp, err := client.Do(&req)
	if err != nil {
		return HdfsJsonData{}, err
	}
	defer rsp.Body.Close()
	hdfsData, err := responseToHdfsData(rsp)
	return hdfsData, err
}
