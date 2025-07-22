package utils

type ContextKey string

const (
	ContextKeyDDSource         ContextKey = "ddsource"
	ContextKeyTarget           ContextKey = "target"
	ContextKeyID               ContextKey = "id"
	ContextKeyParams           ContextKey = "params"
	ContextKeyPrecision        ContextKey = "precision"
	ContextKeyBodyStream       ContextKey = "bodyStream"
	ContextKeyDSN              ContextKey = "DSN"
	ContextKeyMeta             ContextKey = "META"
	ContextKeyTTLDays          ContextKey = "TTL_DAYS"
	ContextKeyAsync            ContextKey = "async"
	ContextKeySplService       ContextKey = "splService"
	ContextKeyTsService        ContextKey = "tsService"
	ContextKeyProfileService   ContextKey = "profileService"
	ContextKeyNode             ContextKey = "node"
	ContextKeySpanAttrsService ContextKey = "spanAttrsService"
	ContextKeySpansService     ContextKey = "spansService"
	ContextKeyFrom             ContextKey = "from"
	ContextKeyName             ContextKey = "name"
	ContextKeyUntil            ContextKey = "until"
)
