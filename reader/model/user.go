package model

import (
	"encoding/json"
	"os"
	"time"
)

var ProxyTokens = make(map[string]int64)

func (TableUser) TableName() string {
	return "users"
}

func (TableUser) TableEngine() string {
	return "ReplacingMergeTree"
}

// swagger:model CreateUserStruct
type TableUser struct {
	UUID string `db:"uuid" csv:"-" clickhouse:"type:UUID;default:generateUUIDv4()" json:"guid"`
	// required: true
	Version uint64 `db:"version" csv:"-" clickhouse:"type:UInt64;default:NOW();key" json:"version" validate:"required,gte=1"`
	//
	UserName string `db:"username" csv:"username" clickhouse:"type:String;order" json:"username" validate:"required,username"`
	// example: 10
	// required: true
	PartID uint16 `db:"partid" csv:"partid" clickhouse:"type:UInt16;default:10" json:"partid" validate:"required,gte=1"`
	// required: true
	Email string `db:"email" csv:"email" clickhouse:"type:String" json:"email" validate:"required,email"`
	// required: true
	Password string `db:"-" csv:"password" json:"password"`
	// required: true
	FirstName string `db:"firstname" csv:"firstname" clickhouse:"type:String" json:"firstname" validate:"required,min=2,ascii"`
	// required: true
	LastName string `db:"lastname" csv:"lastname" clickhouse:"type:String" json:"lastname"`
	// required: true
	// example: NOC
	Department string `db:"department" csv:"department" clickhouse:"type:String" json:"department"`
	// required: true
	// example: admin
	UserGroup     string `db:"usergroup" csv:"usergroup" clickhouse:"type:String" json:"usergroup" validate:"required,alphanum"`
	IsAdmin       bool   `db:"-" csv:"-" json:"-"`
	ExternalAuth  bool   `db:"-" csv:"-" json:"-"`
	ForcePassword bool   `db:"-" csv:"-" json:"-"`

	Params JSONText `db:"params" csv:"params" clickhouse:"type:String" json:"params"`

	Hash string `db:"hash" csv:"passwordhash" clickhouse:"type:String" json:"-"`

	// required: true
	CreatedAt time.Time `db:"record_datetime" csv:"-" clickhouse:"type:DateTime;default:NOW()" json:"-"`

	ExternalProfile string `db:"-" json:"-"`

	Avatar string `db:"-" json:"-"`
}

// swagger:model UserLegacyStruct
type TableUserLegacyFormat struct {
	UserName string `csv:"username" validate:"alphanum"`
	// required: true
	PartID uint16 `csv:"partid" validate:"required,gte=1"`
	// required: true
	Email string `csv:"email" validate:"required,email"`
	// required: true
	Password string `csv:"password"`
	// required: true
	FirstName string `csv:"firstname" validate:"required,alphanum"`
	// required: true
	LastName string `csv:"lastname" validate:"required,alphanum"`
	// required: true
	// example: NOC
	Department string `csv:"department"`
	// example: admin
	UserGroup string `csv:"usergroup" validate:"required,alphanum"`
	//example {}
	Params string `csv:"params"`
	// example: admin
	PasswordHash string `csv:"passwordhash"`
}

// swagger:model UserLoginSuccessResponse
type UserTokenSuccessfulResponse struct {
	// the token
	// example: JWT Token
	Token string `json:"token"`
	// the uuid
	// example: b9f6q23a-0bde-41ce-cd36-da3dbc17ea12
	Scope string `json:"scope"`
	User  struct {
		Admin         bool `json:"admin"`
		ForcePassword bool `json:"force_password"`
	} `json:"user"`
}

// swagger:model UserDetailsResponse
type UserDetailsResponse struct {
	// the uuid
	User struct {
		Admin         bool   `json:"admin"`
		Username      string `json:"username"`
		Usergroup     string `json:"usergroup"`
		ForcePassword bool   `json:"force_password"`
	} `json:"user"`
}

// swagger:model FailureResponse
type UserTokenBadResponse struct {
	// statuscode
	StatusCode int `json:"statuscode"`
	// errot
	Error string `json:"error"`
	// message
	Message string `json:"message"`
}

// swagger:model ListUsers
type GetUser struct {
	// count
	Count int `json:"count"`
	// the data
	Data []*TableUser `json:"data"`
}

// swagger:model UserLogin
type UserloginDetails struct {
	// example: admin
	// required: true
	Username string `json:"username" validate:"required"`
	// example: sipcapture
	// required: true
	Password string `json:"password" validate:"required"`
	// the type of the auth one would like to perform, internal/ldap
	// example: internal
	// required: false
	Type string `json:"type" validate:"-"`
}

// swagger:model UserSuccessResponse
type UserCreateSuccessfulResponse struct {
	// data in JSON format
	//
	// required: true
	//
	// example: af72057b-2745-0a1b-b674-56586aadec57
	Data string `json:"data"`
	// the message for user
	//
	// required: true
	// example: successfully created user
	Message string `json:"message"`
}

// swagger:model UserUpdateSuccessResponse
type UserUpdateSuccessfulResponse struct {
	// example: af72057b-2745-0a1b-b674-56586aadec57
	Data string `json:"data"`
	// example: successfully updated user
	Message string `json:"message"`
}

// swagger:model UserDeleteSuccessResponse
type UserDeleteSuccessfulResponse struct {
	// example: af72057b-2745-0a1b-b674-56586aadec57
	Data string `json:"data"`
	// example: successfully deleted user
	Message string `json:"message"`
}

type HTTPAUTHResp struct {
	Auth bool      `json:"auth" validate:"required"`
	Data TableUser `json:"data" validate:"required"`
}

// swagger:model UserLoginSuccessResponse
type UserProxyTokenData struct {
	// the token
	Token string `json:"token"`
	// required: true
	ExpireAt time.Time `json:"expire_at"`
}

// swagger:model CreateUserStruct
type TableUserPasswordUpdate struct {
	UUID string `db:"-" csv:"-" json:"guid"`
	// required: true
	Password string `db:"-" csv:"password" json:"password"`
	// required: true
	OldPassword string `db:"-" csv:"old_password" json:"old_password"`
}

// swagger:model CreateUserStruct
type UserObject struct {
	UserName string `json:"username"`
	// example: 10
	// required: true
	PartID uint16 `json:"partid"`
	// required: true
	UserGroup string `json:"usergroup"`
}

// swagger:model UserFileUpload
type UserFileUpload struct {
	// in: formData
	// swagger:file
	File os.File
}

// swagger:model UserFileDownload
type UserFileDownload struct {
	// in: body
	// swagger:file
	File os.File
}

// swagger:parameters UserFileResponse UserFileRequest
type UserParameterRequest struct {
	// in: formData
	// swagger:file
	File interface{}
}

//swagger:model TableUserList
type TableUserList struct {
	Data []TableUser `json:"data"`
	// example: 13
	Count int `json:"count"`
}

//swagger:model UserGroupList
type UserGroupList struct {
	// example: ["admin","user"]
	Data []string `json:"data"`
	// example: 13
	Count int `json:"count"`
}

// swagger:model OAuth2TokenExchange
type OAuth2TokenExchange struct {
	// example: token
	// required: true
	OneTimeToken string `json:"token" validate:"required"`
}

// swagger:model OAuth2MapToken
type OAuth2MapToken struct {
	AccessToken string          `json:"access_token"`
	Provider    string          `json:"provider"`
	DataJson    json.RawMessage `json:"datajson"`
	CreateDate  time.Time       `json:"create_date"`
	ExpireDate  time.Time       `json:"expire_date"`
	ProfileJson json.RawMessage `json:"profile_json"`
}
