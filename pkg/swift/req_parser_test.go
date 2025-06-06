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

func Test_trimResellerPrefix(t *testing.T) {
	type args struct {
		account string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "",
			args: args{
				account: "AUTH_account",
			},
			want: "account",
		},
		{
			name: "",
			args: args{
				account: "account",
			},
			want: "account",
		},
		{
			name: "",
			args: args{
				account: "some_account",
			},
			want: "account",
		},
		{
			name: "",
			args: args{
				account: "some-account",
			},
			want: "some-account",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := trimAccountResellerPrefix(tt.args.account); got != tt.want {
				t.Errorf("trimAccountResellerPrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}
