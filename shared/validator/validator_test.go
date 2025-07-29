package validator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/schema"
)

type TestStruct struct {
	Name     string       `json:"name"  schema:"name"  validate:"required,min=1,max=8"`
	Email    string       `json:"email" schema:"email" validate:"required,email,max=256"`
	Age      int          `json:"age"   schema:"age"   validate:"required,gte=18"`
	JoinedAt FlexibleTime `json:"joined_at" schema:"joined_at" validate:"required"`
}

func TestValidate(t *testing.T) {
	t.Parallel()
	refTimeStr := "2023-01-15T10:30:00Z"
	refTime, _ := time.Parse(time.RFC3339, refTimeStr)
	type args struct {
		Name     string       `json:"name"`
		Email    string       `json:"email"`
		Age      int          `json:"age"`
		JoinedAt FlexibleTime `json:"joined_at"`
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		want    TestStruct
	}{
		{
			name: "Valid input",
			args: args{
				Name:     "John Doe",
				Email:    "john@example.com",
				Age:      25,
				JoinedAt: FlexibleTime(refTime),
			},
			wantErr: false,
			want: TestStruct{
				Name:     "John Doe",
				Email:    "john@example.com",
				Age:      25,
				JoinedAt: FlexibleTime(refTime),
			},
		},
		{
			name: "Missing name",
			args: args{
				Name:  "",
				Email: "john@example.com",
				Age:   25,
			},
			wantErr: true,
		},
		{
			name: "Invalid email",
			args: args{
				Name:  "John Doe",
				Email: "invalid-email",
				Age:   25,
			},
			wantErr: true,
		},
		{
			name: "Age below minimum",
			args: args{
				Name:  "John Doe",
				Email: "john@example.com",
				Age:   8,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			jsonData, err := json.Marshal(tt.args)
			if err != nil {
				t.Errorf("cound not marshal json: %v", err)
				return
			}
			got, err := Validate[TestStruct](bytes.NewReader(jsonData))
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			gotTime := got.JoinedAt.Time()
			wantTime := tt.args.JoinedAt.Time()

			if !gotTime.Equal(wantTime) {
				t.Errorf("ValidateQuery() got JoinedAt = %v, want %v", gotTime, wantTime)
			}
			if got.Age != tt.want.Age {
				t.Errorf("ValidateQuery() got Age = %d, want %d", got.Age, tt.want.Age)
			}
			if got.Name != tt.want.Name {
				t.Errorf("ValidateQuery() got Name = %s, want %s", got.Name, tt.want.Name)
			}
			if got.Email != tt.want.Email {
				t.Errorf("ValidateQuery() got Email = %s, want %s", got.Email, tt.want.Email)
			}
			t.Logf("Validate() = %+v", got)
			t.Logf("Validate() error = %v", err)
		})
	}
}

func TestValidate_JSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "Valid json",
			input:   `{"name": "John Doe", "email": "john@example.com", "age":25}`,
			wantErr: false,
		},
		{
			name:    "Invalid json",
			input:   `{"name": "John Doe", "email": "john@example.com", "age":}`,
			wantErr: true,
		},
		{
			name:    "Empty json",
			input:   `{}`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Validate[TestStruct](bytes.NewReader([]byte(tt.input)))
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateQuery(t *testing.T) {
	t.Parallel()
	refTimeStr := "2023-01-15T10:30:00Z"
	refTime, _ := time.Parse(time.RFC3339, refTimeStr)
	refTimeUnix := refTime.Unix()
	type ts struct {
		Name     string       `schema:"name" validate:"required"`
		Email    string       `schema:"email" validate:"required,email"`
		Age      int          `schema:"age" validate:"required,gte=18"`
		JoinedAt FlexibleTime `schema:"joined_at" validate:"required"`
	}
	tests := []struct {
		name    string
		query   url.Values
		want    ts
		wantErr bool
	}{
		{
			name: "Valid input with rfc time",
			query: func() url.Values {
				q := make(url.Values)
				q.Set("name", "John")
				q.Set("email", "john@example.com")
				q.Set("age", "25")
				q.Set("joined_at", refTimeStr)
				return q
			}(),
			wantErr: false,
			want: ts{
				Name:     "John",
				Email:    "john@example.com",
				Age:      25,
				JoinedAt: FlexibleTime(refTime),
			},
		},
		{
			name: "Valid input with timestamp",
			query: func() url.Values {
				q := make(url.Values)
				q.Set("name", "Jane")
				q.Set("email", "jane@example.com")
				q.Set("age", "30")
				q.Set("joined_at", strconv.FormatInt(refTimeUnix, 10))
				return q
			}(),
			wantErr: false,
			want: ts{
				Name:     "Jane",
				Email:    "jane@example.com",
				Age:      30,
				JoinedAt: FlexibleTime(refTime),
			},
		},
		{
			name: "Invalid time format",
			query: func() url.Values {
				q := make(url.Values)
				q.Set("name", "Bob")
				q.Set("email", "bob@example.com")
				q.Set("age", "35")
				q.Set("joined_at", "not-a-time-value")
				return q
			}(),
			wantErr: true,
		},
		{
			name: "Missing name",
			query: func() url.Values {
				q := make(url.Values)
				q.Set("email", "john@example.com")
				q.Set("age", "25")
				q.Set("joined_at", refTimeStr)
				return q
			}(),
			wantErr: true,
		},
		{
			name: "Invalid email",
			query: func() url.Values {
				q := make(url.Values)
				q.Set("name", "John")
				q.Set("email", "invalid-email")
				q.Set("age", "25")
				q.Set("joined_at", refTimeStr)
				return q
			}(),
			wantErr: true,
		},
		{
			name: "Age below minimum",
			query: func() url.Values {
				q := make(url.Values)
				q.Set("name", "John")
				q.Set("email", "john@example.com")
				q.Set("age", "10")
				q.Set("joined_at", refTimeStr)
				return q
			}(),
			wantErr: true,
		},
		{
			name:    "Missing all fields",
			query:   make(url.Values),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ValidateQuery[ts](tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotTime := got.JoinedAt.Time()
			wantTime := tt.want.JoinedAt.Time()

			if !gotTime.Equal(wantTime) {
				t.Errorf("ValidateQuery() got JoinedAt = %v, want %v", gotTime, wantTime)
			}
			if got.Age != tt.want.Age {
				t.Errorf("ValidateQuery() got Age = %d, want %d", got.Age, tt.want.Age)
			}
			if got.Name != tt.want.Name {
				t.Errorf("ValidateQuery() got Name = %s, want %s", got.Name, tt.want.Name)
			}
			if got.Email != tt.want.Email {
				t.Errorf("ValidateQuery() got Email = %s, want %s", got.Email, tt.want.Email)
			}

			t.Logf("ValidateQuery() = %+v", got)
			t.Logf("ValidateQuery() error = %v", err)
		})
	}
}

func TestValidateBody(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		args    TestStruct
		want    TestStruct
		wantErr bool
	}{
		{
			name: "Valid input",
			args: TestStruct{
				Name:  "John Doe",
				Email: "john@example.com",
				Age:   25,
			},
			want: TestStruct{
				Name:  "John Doe",
				Email: "john@example.com",
				Age:   25,
			},
			wantErr: false,
		},
		{
			name: "Missing name",
			args: TestStruct{
				Name:  "",
				Email: "john@example.com",
				Age:   25,
			},
			want:    TestStruct{},
			wantErr: true,
		},
		{
			name: "Invalid email",
			args: TestStruct{
				Name:  "John Doe",
				Email: "invalid-email",
				Age:   25,
			},
			want:    TestStruct{},
			wantErr: true,
		},
		{
			name: "Age below minimum",
			args: TestStruct{
				Name:  "John Doe",
				Email: "john@example.com",
				Age:   8,
			},
			want:    TestStruct{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			jsonData, err := json.Marshal(tt.args)
			if err != nil {
				t.Fatalf("could not marshal json: %v", err)
			}
			req := &http.Request{
				Body: io.NopCloser(bytes.NewReader(jsonData)),
			}
			got, err := ValidateBody[TestStruct](req)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFromRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ValidateFromRequest() = %v, want %v", got, tt.want)
			}
			// Ensure body can still be read and matches the original
			bodyAfter, err := io.ReadAll(req.Body)
			if err != nil {
				t.Errorf("could not read reset request body: %v", err)
			}
			if !bytes.Equal(bodyAfter, jsonData) {
				t.Errorf("request body not properly reset, got %s, want %s", string(bodyAfter), string(jsonData))
			}
		})
	}
}

func TestValidateRequest(t *testing.T) {
	t.Parallel()
	// Define a reference time for testing
	refTimeStr := "2023-01-15T10:30:00Z"
	refTime, _ := time.Parse(time.RFC3339, refTimeStr)
	refTimeUnix := refTime.Unix()
	type TestParams struct {
		Name     string       `schema:"name" validate:"required"`
		Email    string       `schema:"email" validate:"required,email"`
		Age      int          `schema:"age" validate:"required,gte=18"`
		JoinedAt FlexibleTime `schema:"joined_at" validate:"required"`
	}
	tests := []struct {
		name    string
		setup   func() *http.Request
		want    TestParams
		wantErr bool
	}{
		{
			name: "GET with valid parameters",
			setup: func() *http.Request {
				// URL with query parameters
				url := "http://example.com/api?name=John&email=john@example.com&age=25&joined_at=" + refTimeStr

				req, _ := http.NewRequest(http.MethodGet, url, nil)
				return req
			},
			want: TestParams{
				Name:     "John",
				Email:    "john@example.com",
				Age:      25,
				JoinedAt: FlexibleTime(refTime),
			},
			wantErr: false,
		},
		{
			name: "GET with timestamp for time",
			setup: func() *http.Request {
				// URL with query parameters, using timestamp for joined_at
				url := "http://example.com/api?name=Jane&email=jane@example.com&age=30&joined_at=" + fmt.Sprintf("%d", refTimeUnix)

				req, _ := http.NewRequest(http.MethodGet, url, nil)
				return req
			},
			want: TestParams{
				Name:     "Jane",
				Email:    "jane@example.com",
				Age:      30,
				JoinedAt: FlexibleTime(refTime),
			},
			wantErr: false,
		},
		{
			name: "POST with form encoded data only",
			setup: func() *http.Request {
				// Form data only
				formData := url.Values{}
				formData.Set("name", "Bob")
				formData.Set("email", "bob@example.com")
				formData.Set("age", "40")
				formData.Set("joined_at", refTimeStr)

				req, _ := http.NewRequest(
					http.MethodPost,
					"http://example.com/api",
					strings.NewReader(formData.Encode()),
				)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				return req
			},
			want: TestParams{
				Name:     "Bob",
				Email:    "bob@example.com",
				Age:      40,
				JoinedAt: FlexibleTime(refTime),
			},
			wantErr: false,
		},
		{
			name: "POST with combined query and form parameters",
			setup: func() *http.Request {
				// URL with query parameters
				reqUrl := "http://example.com/api?name=Alice&age=35"
				formData := url.Values{}
				formData.Set("email", "alice@example.com")
				formData.Set("joined_at", refTimeStr)

				req, _ := http.NewRequest(
					http.MethodPost,
					reqUrl,
					strings.NewReader(formData.Encode()),
				)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				return req
			},
			want: TestParams{
				Name:     "Alice",
				Email:    "alice@example.com",
				Age:      35,
				JoinedAt: FlexibleTime(refTime),
			},
			wantErr: false,
		},
		{
			name: "GET with missing required field",
			setup: func() *http.Request {
				// URL missing the required name field
				url := "http://example.com/api?email=missing@example.com&age=25"

				req, _ := http.NewRequest(http.MethodGet, url, nil)
				return req
			},
			want:    TestParams{},
			wantErr: true,
		},
		{
			name: "POST with invalid email",
			setup: func() *http.Request {
				// Form data with invalid email
				formData := url.Values{}
				formData.Set("name", "Jane")
				formData.Set("email", "not-an-email")
				formData.Set("age", "25")

				req, _ := http.NewRequest(
					http.MethodPost,
					"http://example.com/api",
					strings.NewReader(formData.Encode()),
				)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				return req
			},
			want:    TestParams{},
			wantErr: true,
		},
		{
			name: "POST with invalid age",
			setup: func() *http.Request {
				// Form data with invalid age (too young)
				formData := url.Values{}
				formData.Set("name", "Young")
				formData.Set("email", "young@example.com")
				formData.Set("age", "17") // Under 18, validation should fail

				req, _ := http.NewRequest(
					http.MethodPost,
					"http://example.com/api",
					strings.NewReader(formData.Encode()),
				)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				return req
			},
			want:    TestParams{},
			wantErr: true,
		},
		{
			name: "GET with invalid time format",
			setup: func() *http.Request {
				// URL with invalid time format
				url := "http://example.com/api?name=TimeError&email=time@example.com&age=25&joined_at=not-a-time"

				req, _ := http.NewRequest(http.MethodGet, url, nil)
				return req
			},
			want:    TestParams{},
			wantErr: true,
		},
		{
			name: "POST with invalid content type",
			setup: func() *http.Request {
				// URL with query parameters
				reqUrl := "http://example.com/api?name=ContentType&email=content@example.com&age=30"

				// Form data that won't be parsed due to wrong content type
				formData := url.Values{}
				formData.Set("joined_at", refTimeStr)

				req, _ := http.NewRequest(
					http.MethodPost,
					reqUrl,
					strings.NewReader(formData.Encode()),
				)
				req.Header.Set("Content-Type", "text/plain") // Not application/x-www-form-urlencoded
				return req
			},
			want: TestParams{
				Name:  "ContentType",
				Email: "content@example.com",
				Age:   30,
				// JoinedAt will be zero since form wasn't parsed
			},
			wantErr: false,
		},
		{
			name: "POST with combined parameters and override",
			setup: func() *http.Request {
				// URL with all parameters
				reqUrl := "http://example.com/api?name=OriginalName&email=original@example.com&age=50&joined_at=" + fmt.Sprintf("%d", refTimeUnix)
				// Form data that will override all parameters
				formData := url.Values{}
				formData.Set("name", "OverrideName")
				formData.Set("email", "override@example.com")
				formData.Set("age", "45")
				formData.Set("joined_at", refTimeStr)

				req, _ := http.NewRequest(
					http.MethodPost,
					reqUrl,
					strings.NewReader(formData.Encode()),
				)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				return req
			},
			want: TestParams{
				Name:     "OverrideName",
				Email:    "override@example.com",
				Age:      45,
				JoinedAt: FlexibleTime(refTime),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := tt.setup()
			got, err := ValidateRequest[TestParams](req)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Compare results
			if got.Name != tt.want.Name {
				t.Errorf("ValidateRequest() Name got = %v, want %v", got.Name, tt.want.Name)
			}
			if got.Email != tt.want.Email {
				t.Errorf("ValidateRequest() Email got = %v, want %v", got.Email, tt.want.Email)
			}
			if got.Age != tt.want.Age {
				t.Errorf("ValidateRequest() Age got = %v, want %v", got.Age, tt.want.Age)
			}
			// For time fields, check if times are equal within a small tolerance
			gotTime := got.JoinedAt.Time()
			wantTime := tt.want.JoinedAt.Time()
			if !gotTime.Equal(wantTime) {
				t.Errorf("ValidateRequest() JoinedAt got = %v, want %v", gotTime, wantTime)
			}

			t.Logf("ValidateRequest() = %+v", got)
			t.Logf("ValidateRequest() error = %+v", err)
		})
	}
}

func TestValidateQuery_OptionalFields(t *testing.T) {
	refTimeStr := "2023-01-15T10:30:00Z"
	refTime, _ := time.Parse(time.RFC3339, refTimeStr)
	refTimeUnix := refTime.Unix()
	type ts struct {
		Start FlexibleTime `schema:"start" flexdefault:"now-6h"`
		End   FlexibleTime `schema:"end" flexdefault:"now"`
	}
	now := time.Now()
	const tolerance = 2 * time.Second
	tests := []struct {
		name      string
		values    url.Values
		wantStart FlexibleTime
		wantEnd   FlexibleTime
		wantErr   bool
	}{
		{
			name: "Both provided (RFC3339 and unix)",
			values: func() url.Values {
				v := make(url.Values)
				v.Set("start", refTimeStr)
				v.Set("end", strconv.FormatInt(refTimeUnix, 10))
				return v
			}(),
			wantStart: FlexibleTime(refTime),
			wantEnd:   FlexibleTime(refTime),
			wantErr:   false,
		},
		{
			name: "Only start provided",
			values: func() url.Values {
				v := make(url.Values)
				v.Set("start", refTimeStr)
				return v
			}(),
			wantStart: FlexibleTime(refTime),
			wantEnd:   FlexibleTime(now), // will fill during test
			wantErr:   false,
		},
		{
			name: "Only end provided",
			values: func() url.Values {
				v := make(url.Values)
				v.Set("end", strconv.FormatInt(refTimeUnix, 10))
				return v
			}(),
			wantStart: FlexibleTime(now.Add(-6 * time.Hour)),
			wantEnd:   FlexibleTime(refTime),
			wantErr:   false,
		},
		{
			name:      "Neither provided (defaults expected)",
			values:    make(url.Values),
			wantStart: FlexibleTime(now.Add(-6 * time.Hour)),
			wantEnd:   FlexibleTime(now),
			wantErr:   false,
		},
		{
			name: "Invalid end format",
			values: func() url.Values {
				v := make(url.Values)
				v.Set("end", "not-a-time")
				return v
			}(),
			wantStart: FlexibleTime{},
			wantEnd:   FlexibleTime{},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateQuery[ts](tt.values)
			t.Logf("ValidateQuery() = %+v", got)
			t.Logf("ValidateQuery() error = %v", err)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotStart := got.Start.Time()
			gotEnd := got.End.Time()
			if !timeAlmostEqual(gotStart, tt.wantStart.Time(), tolerance) {
				t.Errorf("Start mismatch: got %v, want %v", gotStart, tt.wantStart.Time())
			}
			if !timeAlmostEqual(gotEnd, tt.wantEnd.Time(), tolerance) {
				t.Errorf("End mismatch: got %v, want %v", gotEnd, tt.wantEnd.Time())
			}
		})
	}
}

func timeAlmostEqual(a, b time.Time, tolerance time.Duration) bool {
	diff := a.Sub(b)
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}

func TestDecoderFormPrecedence(t *testing.T) {
	decoder := schema.NewDecoder()
	data := url.Values{}
	data["name"] = []string{"post", "query"}
	var s struct{ Name string }
	// decore
	if err := decoder.Decode(&s, data); err != nil {
		panic(err)
	}
	if s.Name != "query" {
		t.Errorf("expected name to be 'query', got '%s'", s.Name)
	}
	t.Logf("Decoded struct: %+v", s)
}
