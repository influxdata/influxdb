package kapacitor

import "testing"

func TestValidateAlert(t *testing.T) {
	tests := []struct {
		name    string
		service string
		wantErr bool
	}{
		{
			name:    "Test valid template alert",
			service: "slack",
			wantErr: false,
		},
		{
			name:    "Test invalid template alert",
			service: "invalid",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		if err := ValidateAlert(tt.service); (err != nil) != tt.wantErr {
			t.Errorf("%q. ValidateAlert() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func Test_validateTick(t *testing.T) {
	type args struct {
		script string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Valid Script",
			args: args{
				script: "stream|from()",
			},
			wantErr: false,
		},
		{
			name: "Invalid Script",
			args: args{
				script: "stream|nothing",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		if err := validateTick(tt.args.script); (err != nil) != tt.wantErr {
			t.Errorf("%q. validateTick() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}
