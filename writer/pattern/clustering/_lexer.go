//go:generate re2go -o lexer.go _lexer.go
package clustering

/*!re2c
  re2c:define:YYCTYPE = byte;
  re2c:define:YYPEEK = "peek(inputBytes, cursor)";
  re2c:define:YYSKIP = "cursor += 1";
  re2c:define:YYBACKUP = "marker = cursor";
  re2c:define:YYRESTORE = "cursor = marker";
  re2c:yyfill:enable = 0;
  re2c:flags:utf-8 = 1;
*/

func Lex(input string, tokens []Token) []Token {
	cursor := 0
	marker := 0
	limit := len(input)

	// Convert string to byte slice for re2c compatibility
	inputBytes := []byte(input)

	for cursor < limit {
		tokenStart := cursor

		/*!re2c
		// Whitespace (mapped to Special type)
		[ \t\r\n]+ {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: Special})
		   }
		   continue
		}
		// UUID
		[0-9a-fA-F]{8}"-"[0-9a-fA-F]{4}"-"[0-9a-fA-F]{4}"-"[0-9a-fA-F]{4}"-"[0-9a-fA-F]{12} {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: UUID})
		   }
		   continue
		}
		// RFC 5424 Priority field (e.g., <34>)
		"<" [0-9]+ ">" {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: Priority})
		   }
		   continue
		}
		// Timestamp formats (common in logs)
		// ISO8601 timestamps (RFC 5424)
		[0-9]{4} "-" [0-9]{2} "-" [0-9]{2} "T" [0-9]{2} ":" [0-9]{2} ":" [0-9]{2} ("." [0-9]+)? ("Z"|[+-][0-9]{2}":"[0-9]{2})? {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: Timestamp})
		   }
		   continue
		}
		// Common log format timestamp [day/month/year:hour:minute:second zone]
		"[" [0-9]{2} "/" [a-zA-Z]{3} "/" [0-9]{4} ":" [0-9]{2} ":" [0-9]{2} ":" [0-9]{2} [ +-][0-9]{4} "]" {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: Timestamp})
		   }
		   continue
		}
		// BSD syslog timestamp format (e.g., Jan 23 14:59:01)
		[A-Za-z]{3} [ ][0-9]{1,2} [ ][0-9]{2}":"[0-9]{2}":"[0-9]{2} {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: Timestamp})
		   }
		   continue
		}
		// Log levels
		"INFO"|"DEBUG"|"WARN"|"WARNING"|"ERROR"|"CRITICAL"|"FATAL"|"NOTICE"|"EMERGENCY"|"ALERT" {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: LogLevel})
		   }
		   continue
		}
		// Program name with optional PID (e.g., "sshd[12345]:")
		[a-zA-Z][a-zA-Z0-9_-]+ "[" [0-9]+ "]" ":" {
		   if cursor <= limit {
		       // Extract program name and PID separately
		       pidStart := -1
		       pidEnd := -1
		       for i := tokenStart; i < cursor; i++ {
		           if inputBytes[i] == '[' {
		               pidStart = i + 1
		           } else if inputBytes[i] == ']' {
		               pidEnd = i
		               break
		           }
		       }
		       if pidStart > -1 && pidEnd > -1 {
		           tokens = append(tokens, Token{Value: input[tokenStart:pidStart-1], Type: ProgramName})
		           tokens = append(tokens, Token{Value: input[pidStart:pidEnd], Type: PID})
		           tokens = append(tokens, Token{Value: input[pidEnd:cursor], Type: Special})
		       } else {
		           tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: ProgramName})
		       }
		   }
		   continue
		}
		// HTTP path part
		("/"[a-zA-Z0-9_-]+|"/ ") {
		          if input[cursor - 1] == ' ' {
		             cursor--
		          }
		    if cursor <= limit {
		                 tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: HTTPPathPart})
		             }
		       continue
		}
		// HTTP version
		"HTTP"("/"[12]"."[01])? {
		    if cursor <= limit {
		                tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: HTTPVersion})
		            }
		      continue
		}
		// IP Address pattern (IPv4)
		[0-9]+ "." [0-9]+ "." [0-9]+ "." [0-9]+ {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: IPAddress})
		   }
		   continue
		}
		// HTTP Methods
		"GET"|"POST"|"PUT"|"DELETE"|"PATCH"|"HEAD"|"OPTIONS"|"CONNECT"|"TRACE" {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: HTTPMethod})
		   }
		   continue
		}
		// number (integer or float or HTTP code)
		[0-9]+("."[0-9]+)? {
		   if cursor <= limit {
				tp := Number
				if isHTTPCode(input[tokenStart:cursor]) { tp = HTTPCode }
					tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: tp})
		   		}
		   continue
		}
		// Words (letters only)
		[a-zA-Z]+ {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: Word})
		   }
		   continue
		}
		// Special characters
		[.,!?;:'"()\[\]{}] {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: input[tokenStart:cursor], Type: Special})
		   }
		   continue
		}
		// Anything else (fall back to Special type)
		[^] {
		   if cursor <= limit {
		       tokens = append(tokens, Token{Value: string(inputBytes[cursor-1]), Type: Special})
		   }
		   continue
		}
		*/
	}

	return tokens
}

// Helper function to safely peek at bytes
func peek(input []byte, pos int) byte {
	if pos >= len(input) {
		return 0 // Return null byte if out of bounds
	}
	return input[pos]
}

// Helper function to check if a byte is a digit
func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func isHTTPCode(b string) bool {
	return len(b) == 3 && b[0] >= '0' && b[0] <= '5'
}
