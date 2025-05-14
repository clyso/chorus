package swift

import (
	"net/http"
	"strings"
)

func ParseReq(r *http.Request) (account string, container string, object string, method Method) {
	path := strings.Trim(r.URL.Path, "/")
	// parse swift info endpoint
	if !strings.Contains(path, "v1/") {
		if r.Method == "GET" && strings.Contains(path, "info") {
			return "", "", "", GetInfo
		}
		return "", "", "", UndefinedMethod
	}
	path = trimV1(path)
	if r.Method == "GET" && path == "endpoints" {
		return "", "", "", GetEndpoints
	}
	paths := strings.Split(path, "/")
	switch len(paths) {
	case 1:
		// account requests:
		account = paths[0]
		method = parseAccount(r.Method)
	case 2:
		// container requests:
		account, container = paths[0], paths[1]
		method = parseContainer(r.Method)
	case 3:
		// object requests:
		account, container, object = paths[0], paths[1], paths[2]
		method = parseObject(r.Method)
	default:
		// invalid request
		return "", "", "", UndefinedMethod
	}
	return account, container, object, method
}

func parseAccount(httpMethod string) Method {
	switch httpMethod {
	case "GET":
		return GetAccount
	case "POST":
		return PostAccount
	case "HEAD":
		return HeadAccount
	case "DELETE":
		return DeleteAccount
	default:
		return UndefinedMethod
	}
}

func parseContainer(httpMethod string) Method {
	switch httpMethod {
	case "GET":
		return GetContainer
	case "PUT":
		return PutContainer
	case "POST":
		return PostContainer
	case "HEAD":
		return HeadContainer
	case "DELETE":
		return DeleteContainer
	default:
		return UndefinedMethod
	}
}

func parseObject(httpMethod string) Method {
	switch httpMethod {
	case "GET":
		return GetObject
	case "PUT":
		return PutObject
	case "COPY":
		return CopyObject
	case "DELETE":
		return DeleteObject
	case "HEAD":
		return HeadObject
	case "POST":
		return PostObject
	default:
		return UndefinedMethod
	}
}

// remove all symbols in path before v1/ incuding v1/
func trimV1(path string) string {
	return path[strings.Index(path, "v1/")+len("v1/"):]
}
