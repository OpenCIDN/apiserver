package controller

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/OpenCIDN/apiserver/pkg/versions"
)

func httpStat(url string, client *http.Client, headers map[string]string) (*httpFileInfo, error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", versions.DefaultUserAgent())
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, request: %+v, response: %+v, body: %s", resp.StatusCode, req, resp, string(body))
	}

	contentLength := resp.Header.Get("Content-Length")
	size, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid Content-Length: %v", err)
	}

	lastModified, err := http.ParseTime(resp.Header.Get("Last-Modified"))
	if err != nil {
		lastModified = time.Time{} // Use zero time if parsing fails
	}

	return &httpFileInfo{
		Size:    size,
		ModTime: lastModified,
		Range:   resp.Header.Get("Accept-Ranges") == "bytes",
	}, nil
}

type httpFileInfo struct {
	Size    int64
	ModTime time.Time
	Range   bool
}
