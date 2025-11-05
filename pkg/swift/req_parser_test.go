package swift

import (
	"testing"
)

func Test_trimV1(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "",
			args: args{
				path: "some/path/v1/123",
			},
			want: "123",
		},
		{
			name: "",
			args: args{
				path: "/v1/123",
			},
			want: "123",
		},
		{
			name: "",
			args: args{
				path: "v1/123",
			},
			want: "123",
		},
		{
			name: "",
			args: args{
				path: "v1/v1/123",
			},
			want: "v1/123",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := trimV1(tt.args.path); got != tt.want {
				t.Errorf("trimV1() = %v, want %v", got, tt.want)
			}
		})
	}
}
