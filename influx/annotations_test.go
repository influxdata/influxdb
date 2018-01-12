package influx

/*
func TestLineProtocol_Annotation(t *testing.T) {
	type fields struct {
		Measurement string
	}
	type args struct {
		a *chronograf.Annotation
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "creates line protocol from annotation",
			fields: fields{
				Measurement: "mymeasurement",
			},
			args: args{
				a: &chronograf.Annotation{
					Name:     "myname",
					Type:     "mytype",
					Time:     1234,
					Duration: 5,
					Text:     "mytext",
				},
			},
			want: `mymeasurement,type=mytype,name=myname deleted=false,duration_ns=5i,text="mytext" 1234\n`,
		},
		{
			name: "creates line protocol from annotation with default fields",
			fields: fields{
				Measurement: "mymeasurement",
			},
			args: args{
				a: &chronograf.Annotation{
					Name: "myname",
					Type: "mytype",
				},
			},
			want: `mymeasurement,type=mytype,name=myname deleted=false,duration_ns=0i,text="" 0\n`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := LineProtocol{
				Measurement: tt.fields.Measurement,
			}
			if got := l.Annotation(tt.args.a); got != tt.want {
				t.Errorf("LineProtocol.Annotation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLineProtocol_DeleteAnnotation(t *testing.T) {
	type fields struct {
		Measurement string
	}
	type args struct {
		a *chronograf.Annotation
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "sets the delete flag in the annotation",
			fields: fields{
				Measurement: "mymeasurement",
			},
			args: args{
				a: &chronograf.Annotation{
					Name:     "myname",
					Type:     "mytype",
					Time:     1234,
					Duration: 5,
					Text:     "mytext",
				},
			},
			want: `mymeasurement,type=mytype,name=myname deleted=true,duration_ns=0i,text="" 1234\n`,
		},
		{
			name: "ignores default field values",
			fields: fields{
				Measurement: "mymeasurement",
			},
			args: args{
				a: &chronograf.Annotation{
					Name: "myname",
					Type: "mytype",
					Time: 1234,
				},
			},
			want: `mymeasurement,type=mytype,name=myname deleted=true,duration_ns=0i,text="" 1234\n`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := LineProtocol{
				Measurement: tt.fields.Measurement,
			}
			if got := l.DeleteAnnotation(tt.args.a); got != tt.want {
				t.Errorf("LineProtocol.DeleteAnnotation() = %v, want %v", got, tt.want)
			}
		})
	}
}
*/
