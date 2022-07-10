package radosAPI

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// API contains fields to communicate with the rados-gateway
type API struct {
	host      string
	accessKey string
	secretKey string
	prefix    string
	client    *http.Client
}

// New returns client for Ceph RADOS Gateway
func New(host, accessKey, secretKey string, adminPrefix ...string) (*API, error) {
	return NewWithClient(&http.Client{}, host, accessKey, secretKey, adminPrefix...)
}

func NewWithClient(client *http.Client, host, accessKey, secretKey string, adminPrefix ...string) (*API, error) {
	prefix := "admin"
	if len(adminPrefix) > 0 {
		prefix = adminPrefix[0]
	}
	if host == "" || accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("host, accessKey, secretKey must be not nil")
	}
	return &API{host, accessKey, secretKey, prefix, client}, nil
}

func (api *API) makeRequest(verb, url string) (body []byte, statusCode int, err error) {
	var apiErr apiError

	// fmt.Printf("URL [%v]: %v\n", verb, url)
	req, err := http.NewRequest(verb, url, nil)
	if err != nil {
		return
	}
	signer := v4.NewSigner()
	credentials := aws.Credentials{
		AccessKeyID:     api.accessKey,
		SecretAccessKey: api.secretKey,
	}

	payloadHash := getRequestBodySHA256(req)

	// For Ceph, region name does not matter and the service is always S3
	signer.SignHTTP(context.TODO(), credentials, req, payloadHash, "s3", "us-east-1", time.Now())
	resp, err := api.client.Do(req)
	if err != nil {
		return
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	statusCode = resp.StatusCode
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if errMarshal := json.Unmarshal(body, &apiErr); errMarshal == nil && apiErr.Code != "" {
		err = errors.New(apiErr.Code)
	}
	return
}

func (api *API) call(verb, route string, args url.Values, usePrefix bool, sub ...string) (body []byte, statusCode int, err error) {
	subreq := ""
	if len(sub) > 0 {
		subreq = fmt.Sprintf("%s&", sub[0])
	}
	if usePrefix {
		route = fmt.Sprintf("/%s%s", api.prefix, route)
	}
	body, statusCode, err = api.makeRequest(verb, fmt.Sprintf("%v%v?%v%s", api.host, route, subreq, args.Encode()))
	if statusCode != 200 {
		err = fmt.Errorf("[%v]: %v", statusCode, err)
	}
	return
}

func getRequestBodySHA256(request *http.Request) string {
	if request.Body == nil {
		// SHA256 hash of empty string
		return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	}
	payload, _ := ioutil.ReadAll(request.Body)
	request.Body = ioutil.NopCloser(bytes.NewReader(payload))

	return string(sha256.New().Sum(payload))
}
