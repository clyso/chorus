package swift

//go:generate go tool stringer -type Method

type Method uint8

const (
	UndefinedMethod Method = iota
	GetInfo
	GetAccount
	PostAccount
	HeadAccount
	DeleteAccount

	GetContainer
	PutContainer
	PostContainer
	HeadContainer
	DeleteContainer

	GetObject
	PutObject
	CopyObject
	DeleteObject
	HeadObject
	PostObject

	GetEndpoints
)
