package shared

// Synthetic labels injected onto a log entry when a parser stage fails to
// process its line, mirroring Loki's behavior. They are ordinary labels and so
// are filterable with the usual label filters (e.g. `| __error__=""`).
const (
	ErrorLabel        = "__error__"
	ErrorDetailsLabel = "__error_details__"
)

// Values set in ErrorLabel, one per parser kind.
const (
	JSONParserErr   = "JSONParserErr"
	LogfmtParserErr = "LogfmtParserErr"
)

// ParserErrorType maps a parser op name to the ErrorLabel value it emits on
// failure. Parsers absent from the map (e.g. regexp) do not flag runtime errors.
var ParserErrorType = map[string]string{
	"json":   JSONParserErr,
	"logfmt": LogfmtParserErr,
}
