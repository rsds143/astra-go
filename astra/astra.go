package astra

import (
	"fmt"
	"net/http"
)

// Authenticate connects to the Astra service and gets a bearer token, stores this
// and then returns an AuthenticatedClient, failure to authenticate will return an error
func Authenticate(region, database, username, password string) (*AuthenticatedClient, error) {
	c := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxConnsPerHost:     10,
			MaxIdleConnsPerHost: 10,
			Dial: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 10 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	body, err := json.Marshal(map[string]interface{}{
		"username": username,
		"password": password,
	})
	if err != nil {
		return &AuthenticatedClient{}, fmt.Errorf("unable to marshal JSON object with: %w", err)
	}
	url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v1/auth", database, region)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return &AuthenticatedClient{}, fmt.Errorf("failed creating request with: %w", err)

	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	res, err := c.Do(req)
	if err != nil {
		return &AuthenticatedClient{}, fmt.Errorf("failed logging into Astra with: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Warnf("unable to close body: %s", err)
		}
	}()

	var payload map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&payload)
	if err != nil {
		return &AuthenticatedClient{}, fmt.Errorf("unable to decode response with error: %w", err)
	}
	if token, ok := payload["authToken"]; !ok {
		return &AuthenticatedClient{}, fmt.Errorf("unable to find authtoken in json: %s", payload)
	} else {
		return &AuthenticatedClient{
			client: c,
			token:  fmt.Sprintf("%s", token),
		}, nil
	}
}
