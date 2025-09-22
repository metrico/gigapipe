package logql_parser

import (
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

func TestParser(t *testing.T) {
	tests := []string{
		"{test_id=\"${testID}\"}",
		"{test_id=\"${testID}\", freq=\"2\"}",
		"{test_id=\"${testID}\", freq=\"2\"} |~ \"2[0-9]$\"",
		"rate({test_id=\"${testID}\", freq=\"2\"} |~ \"2[0-9]$\" [1s])",
		"sum by (test_id) (rate({test_id=\"${testID}\"} |~ \"2[0-9]$\" [1s]))",
		"rate({test_id=\"${testID}\", freq=\"2\"} |~ \"2[0-9]$\" [1s])",
		"sum by (test_id) (rate({test_id=\"${testID}\"} |~ \"2[0-9]$\" [1s]))",
		"{test_id=\"${testID}_json\"}|json",
		"{test_id=\"${testID}_json\"}|json lbl_repl=\"new_lbl\"",
		"{test_id=\"${testID}_json\"}|json lbl_repl=\"new_lbl\"|lbl_repl=\"new_val\"",
		"{test_id=\"${testID}_json\"}|json lbl_repl=\"new_lbl\"|fmt=\"json\"",
		"{test_id=\"${testID}_json\"}|json|fmt=~\"[jk]son\"",
		"{test_id=\"${testID}_json\"}|json|lbl_repl=\"REPL\"",
		"sum_over_time({test_id=\"${testID}_json\"}|json|lbl_repl=\"REPL\"|unwrap int_lbl [3s]) by (test_id, lbl_repl)",
		"sum_over_time({test_id=\"${testID}_json\"}|json lbl_int1=\"int_val\"|lbl_repl=\"val_repl\"|unwrap lbl_int1 [3s]) by (test_id, lbl_repl)",
		"{test_id=\"${testID}\"}| line_format \"{ \\\"str\\\":\\\"{{_entry}}\\\", \\\"freq2\\\": {{divide freq 2}} }\"",
		"rate({test_id=\"${testID}\"}| line_format \"{ \\\"str\\\":\\\"{{_entry}}\\\", \\\"freq2\\\": {{divide freq 2}} }\"| json|unwrap freq2 [1s]) by (test_id, freq2)",
		"{test_id=\"${testID}_json\"}|json|json int_lbl2=\"int_val\"",
		"{test_id=\"${testID}_json\"}| line_format \"{{ divide test_id 2  }}\"",
		"rate({test_id=\"${testID}_json\"}| line_format \"{{ divide int_lbl 2  }}\" | unwrap _entry [1s])",
		"sum(rate({test_id=\"${testID}_json\"}| json [5s])) by (test_id)",
		"sum(rate({test_id=\"${testID}_json\"}| json lbl_rrr=\"lbl_repl\" [5s])) by (test_id, lbl_rrr)",
		"sum(sum_over_time({test_id=\"${testID}_json\"}| json | unwrap int_val [10s]) by (test_id, str_id)) by (test_id)",
		"rate({test_id=\"${testID}\"} [1s]) == 2",
		"sum(rate({test_id=\"${testID}\"} [1s])) by (test_id) > 4",
		"sum(sum_over_time({test_id=\"${testID}_json\"}| json | unwrap str_id [10s]) by (test_id, str_id)) by (test_id) > 1000",
		"rate({test_id=\"${testID}\"} | line_format \"12345\" [1s]) == 2",
		"{test_id=\"${testID}\"} | freq >= 4",
		"{test_id=\"${testID}_json\"} | json sid=\"str_id\" | sid >= 598",
		"{test_id=\"${testID}_json\"} | json | str_id >= 598",
		"{test_id=\"${testID}\"} | regexp \"^(?<e>[^0-9]+)[0-9]+$\"",
		"{test_id=\"${testID}\"} | regexp \"^[^0-9]+(?<e>[0-9])+$\"",
		"{test_id=\"${testID}\"} | regexp \"^[^0-9]+([0-9]+(?<e>[0-9]))$\"",
		"first_over_time({test_id=\"${testID}\", freq=\"0.5\"} | regexp \"^[^0-9]+(?<e>[0-9]+)$\" | unwrap e [1s]) by(test_id)",
		"{test_id=\"${testID}\"} | freq > 1 and (freq=\"4\" or freq==2 or freq > 0.5)",
		"{test_id=\"${testID}_json\"} | json sid=\"str_id\" | sid >= 598 or sid < 2 and sid > 0",
		"{test_id=\"${testID}_json\"} | json | str_id < 2 or str_id >= 598 and str_id > 0",
		"{test_id=\"${testID}_json\"} | json | drop a, b, __C__, d=\"e\"",
		"count_over_time({test_id=\"${testID}_json\"} [1m] offset 1m)",
	}
	asts := make([]*LogQLScript, len(tests))
	for i, str := range tests {
		ast, err := Parse(str)
		if err != nil {
			fmt.Printf("[%d]: %s\n", i, str)
			t.Fatal(err)
		}
		asts[i] = ast
	}
	cupaloy.SnapshotT(t, asts)
}

func TestQuotedString_String(t *testing.T) {
	res := "abcabc\" `   d"
	str, err := (&QuotedString{Str: "\"abcabc\\\" `   d\""}).Unquote()
	if err != nil {
		t.Fatal(err)
	}
	if str != res {
		t.Fatalf("%s != %s", str, res)
	}
	str, err = (&QuotedString{Str: "`abcabc\" \\`   d`"}).Unquote()
	if err != nil {
		t.Fatal(err)
	}
	if str != res {
		t.Fatalf("%s != %s", str, res)
	}
}

func TestParser2(t *testing.T) {
	ast, err := Parse(`{sender="logtest"} |= "GET"`)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(ast.String())
}

func TestFindFirst(t *testing.T) {
	ast, err := Parse(`{sender="logtest"} |= "GET"`)
	if err != nil {
		t.Fatal(err)
	}
	strSel := FindFirst[LineFilter](ast)
	fmt.Println(strSel)
}
