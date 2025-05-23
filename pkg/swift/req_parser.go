package swift

import (
	"net/http"
	"strings"

	"github.com/rs/zerolog"
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
	paths := strings.SplitN(path, "/", 3)
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
		account, container, object = paths[0], paths[1], strings.Join(paths[2:], "/")
		method = parseObject(r.Method)
		// in swift copy object method destination set in header:
		if method == CopyObject {
			sameName := true
			destAcc := r.Header.Get("Destination-Account")
			if destAcc != "" && destAcc != account {
				//TODO: should we check that source and dest accounts are routed to the same storage???
				account = destAcc
				sameName = false
			}
			// get copy destination:
			dest := r.Header.Get("Destination")
			dest = strings.Trim(dest, "/")
			destContObj := strings.SplitN(dest, "/", 2)
			if len(destContObj) != 2 {
				zerolog.Ctx(r.Context()).Warn().Msgf("invalid swift copy obj destination: %q", dest)
				// invalid request: copy should contain container and object in destination
				return "", "", "", UndefinedMethod
			}
			// check if source equals to destination:
			sameName = sameName && container == destContObj[0] && object == destContObj[1]
			if sameName {
				// according to swift spec if copy object has the same name and destination,
				// then obj content is not updated and method is equal to PostObject
				// where only obj meta is updated
				method = PostObject
			}
			container, object = destContObj[0], destContObj[1]
		}

	default:
		// should not be possible
		zerolog.Ctx(r.Context()).Warn().Msgf("invalid swift method path: %q", r.URL.Path)
		// invalid request
		return "", "", "", UndefinedMethod
	}
	account = strings.TrimPrefix(account, "AUTH_")
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
