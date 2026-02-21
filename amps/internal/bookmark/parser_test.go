package bookmark

import "testing"

func TestNormalizeSubIDCoverage(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"   ", ""},
		{",,,", ""},
		{"sub-1", "sub-1"},
		{"  sub-1  ", "sub-1"},
		{" , \t , sub-2 , sub-3", "sub-2"},
		{"\n\r\t", ""},
		{" , \tfoo\t ", "foo"},
		{"sub-1,sub-2", "sub-1"},
	}

	for _, tc := range cases {
		if got := NormalizeSubID(tc.input); got != tc.want {
			t.Fatalf("NormalizeSubID(%q)=%q want %q", tc.input, got, tc.want)
		}
	}
}
