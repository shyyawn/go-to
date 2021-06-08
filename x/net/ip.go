package net

import (
	log "github.com/shyyawn/go-to/x/logging"
	"net/http"
	"strings"
)

func GetIP(request *http.Request) string {
	ips := (*request).Header.Get("X-Forwarded-For")
	ip := strings.Split(ips, ",")[0]
	if ip == "" {
		log.Debug("No forwarded IP")
		ip = (*request).RemoteAddr
		if strings.Index(ip, ":") != -1 {
			ip = strings.Split((*request).RemoteAddr, ":")[0]
		}
	}
	return ip
}
