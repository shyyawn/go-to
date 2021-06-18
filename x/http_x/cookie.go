package http_x

import "net/http"

func GetCookie(request *http.Request, name string) string {
	cookie, err := (*request).Cookie(name)
	if err != nil || cookie == nil {
		return ""
	}
	return cookie.Value
}
