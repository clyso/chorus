package notifications

import "testing"

func TestUserIDFromNotificationID(t *testing.T) {
	type args struct {
		bucket string
		user   string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "simple case",
			args: args{
				bucket: "bucket",
				user:   "user",
			},
			want:    "user",
			wantErr: false,
		},
		{
			name: "bucket with dash",
			args: args{
				bucket: "bucket-name",
				user:   "user",
			},
			want:    "user",
			wantErr: false,
		},
		{
			name: "user with dash",
			args: args{
				bucket: "bucket",
				user:   "user-name",
			},
			want:    "user-name",
			wantErr: false,
		},
		{
			name: "user and bucket with dash",
			args: args{
				bucket: "bucket-name",
				user:   "user-name",
			},
			want:    "user-name",
			wantErr: false,
		},
		{
			name: "bucket with 2 dash",
			args: args{
				bucket: "bucket--name",
				user:   "user",
			},
			want:    "user",
			wantErr: false,
		},
		{
			name: "user with 2 dash",
			args: args{
				bucket: "bucket",
				user:   "user--name",
			},
			want:    "user--name",
			wantErr: false,
		},
		{
			name: "user and bucket with 2 dash",
			args: args{
				bucket: "bucket--name",
				user:   "user--name",
			},
			want:    "user--name",
			wantErr: false,
		},
		{
			name: "bucket with 3 dash",
			args: args{
				bucket: "bucket---name",
				user:   "user",
			},
			want:    "user",
			wantErr: false,
		},
		{
			name: "user with 3 dash",
			args: args{
				bucket: "bucket",
				user:   "user---name",
			},
			want:    "user---name",
			wantErr: false,
		},
		{
			name: "user and bucket with 3 dash",
			args: args{
				bucket: "bucket---name",
				user:   "user---name",
			},
			want:    "user---name",
			wantErr: false,
		},
		{
			name: "bucket with mixed dash",
			args: args{
				bucket: "bucket-na--m---e",
				user:   "user",
			},
			want:    "user",
			wantErr: false,
		},
		{
			name: "user with mixed dash",
			args: args{
				bucket: "bucket",
				user:   "user-na--m---e",
			},
			want:    "user-na--m---e",
			wantErr: false,
		},
		{
			name: "user and bucket with mixed dash",
			args: args{
				bucket: "bucket-na--m---e",
				user:   "user-na--m---e",
			},
			want:    "user-na--m---e",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := notificationID(tt.args.user, tt.args.bucket)
			got, err := UserIDFromNotificationID(id)
			if (err != nil) != tt.wantErr {
				t.Errorf("UserIDFromNotificationID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("UserIDFromNotificationID() got = %v, want %v", got, tt.want)
			}
		})
	}
}
