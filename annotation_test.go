package influxdb

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/stretchr/testify/require"
)

var (
	testTime  time.Time = time.Now()
	testTime2 time.Time = testTime.Add(time.Minute)

	annID, _ = platform.IDFromString("2345678901234567")
)

func nowFunc() time.Time {
	return testTime
}

func TestAnnotationCreate(t *testing.T) {
	type tst struct {
		name     string
		input    AnnotationCreate
		expected AnnotationCreate
		err      *errors.Error
	}

	tests := []tst{
		{
			name: "minimum valid create request",
			input: AnnotationCreate{
				Summary: "this is a default annotation",
			},
			expected: AnnotationCreate{
				StreamTag: "default",
				Summary:   "this is a default annotation",
				EndTime:   &testTime,
				StartTime: &testTime,
			},
		},
		{
			name: "full valid create request",
			input: AnnotationCreate{
				StreamTag: "other",
				Summary:   "this is another annotation",
				Message:   "This is a much longer description or message to add to the annotation summary",
				Stickers:  map[string]string{"product": "cloud"},
				EndTime:   &testTime2,
				StartTime: &testTime,
			},
			expected: AnnotationCreate{
				StreamTag: "other",
				Summary:   "this is another annotation",
				Message:   "This is a much longer description or message to add to the annotation summary",
				Stickers:  map[string]string{"product": "cloud"},
				EndTime:   &testTime2,
				StartTime: &testTime,
			},
		},
		{
			name:  "empty create request",
			input: AnnotationCreate{},
			err:   errEmptySummary,
		},
		{
			name: "end time before start create request",
			input: AnnotationCreate{
				Summary:   "this is a default annotation",
				EndTime:   &testTime,
				StartTime: &testTime2,
			},
			err: errReversedTimes,
		},
		{
			name: "default end time before start create request",
			input: AnnotationCreate{
				Summary:   "this is a default annotation",
				StartTime: &testTime2,
			},
			err: errReversedTimes,
		},
		{
			name: "summary too long",
			input: AnnotationCreate{
				Summary: strings.Repeat("a", 256),
			},
			err: errSummaryTooLong,
		},
		{
			name: "message too long",
			input: AnnotationCreate{
				Summary: "longTom",
				Message: strings.Repeat("a", 4097),
			},
			err: errMsgTooLong,
		},
		{
			name: "stream tag too long",
			input: AnnotationCreate{
				Summary:   "longTom",
				StreamTag: strings.Repeat("a", 256),
			},
			err: errStreamTagTooLong,
		},
		{
			name: "sticker key too long",
			input: AnnotationCreate{
				Summary:  "longTom",
				Stickers: map[string]string{strings.Repeat("1", 256): "val"},
			},
			err: errStickerTooLong,
		},
		{
			name: "sticker val too long",
			input: AnnotationCreate{
				Summary:  "longTom",
				Stickers: map[string]string{"key": strings.Repeat("1", 256)},
			},
			err: errStickerTooLong,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.input.Validate(nowFunc)
			if test.err != nil {
				require.Equal(t, test.err, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expected, test.input)
		})
	}
}

func TestDeleteFilter(t *testing.T) {
	type tst struct {
		name     string
		input    AnnotationDeleteFilter
		expected AnnotationDeleteFilter
		err      *errors.Error
	}

	tests := []tst{
		{
			name: "minimum valid delete",
			input: AnnotationDeleteFilter{
				StreamTag: "default",
				EndTime:   &testTime,
				StartTime: &testTime,
			},
			expected: AnnotationDeleteFilter{
				StreamTag: "default",
				EndTime:   &testTime,
				StartTime: &testTime,
			},
		},
		{
			name: "full valid delete",
			input: AnnotationDeleteFilter{
				StreamTag: "default",
				Stickers:  map[string]string{"product": "oss"},
				EndTime:   &testTime,
				StartTime: &testTime,
			},
			expected: AnnotationDeleteFilter{
				StreamTag: "default",
				Stickers:  map[string]string{"product": "oss"},
				EndTime:   &testTime,
				StartTime: &testTime,
			},
		},
		{
			name: "missing stream tag",
			input: AnnotationDeleteFilter{
				Stickers:  map[string]string{"product": "oss"},
				EndTime:   &testTime,
				StartTime: &testTime,
			},
			err: errMissingStreamTagOrId,
		},
		{
			name: "missing start time",
			input: AnnotationDeleteFilter{
				StreamTag: "default",
				Stickers:  map[string]string{"product": "oss"},
				EndTime:   &testTime,
			},
			err: errMissingStartTime,
		},
		{
			name: "missing end time",
			input: AnnotationDeleteFilter{
				StreamTag: "default",
				Stickers:  map[string]string{"product": "oss"},
				StartTime: &testTime,
			},
			err: errMissingEndTime,
		},
		{
			name: "end time before start create request",
			input: AnnotationDeleteFilter{
				StreamTag: "default",
				Stickers:  map[string]string{"product": "oss"},
				EndTime:   &testTime,
				StartTime: &testTime2,
			},
			err: errReversedTimes,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.input.Validate()
			if test.err != nil {
				require.Equal(t, test.err, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expected, test.input)
		})
	}
}

func TestAnnotationListFilter(t *testing.T) {
	type tst struct {
		name       string
		input      AnnotationListFilter
		expected   AnnotationListFilter
		checkValue bool
		err        *errors.Error
	}

	tests := []tst{
		{
			name: "minimum valid",
			input: AnnotationListFilter{
				BasicFilter: BasicFilter{
					EndTime:   &testTime,
					StartTime: &testTime,
				},
			},
			expected: AnnotationListFilter{
				BasicFilter: BasicFilter{
					EndTime:   &testTime,
					StartTime: &testTime,
				},
			},
		},
		{
			name:  "empty valid",
			input: AnnotationListFilter{},
			expected: AnnotationListFilter{
				BasicFilter: BasicFilter{
					EndTime:   &testTime,
					StartTime: &testTime,
				},
			},
			checkValue: true,
		},
		{
			name: "invalid due to reversed times",
			input: AnnotationListFilter{
				BasicFilter: BasicFilter{
					EndTime:   &testTime,
					StartTime: &testTime2,
				},
			},
			err: errReversedTimes,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.input.Validate(nowFunc)
			if test.err != nil {
				require.Equal(t, test.err, err)
				return
			}

			require.NoError(t, err)
			if test.checkValue {
				require.Equal(t, *test.expected.BasicFilter.StartTime, *test.expected.BasicFilter.EndTime)
			} else {
				require.Equal(t, test.expected, test.input)
			}
		})
	}
}

func TestStreamIsValid(t *testing.T) {
	type tst struct {
		name  string
		input Stream
		err   *errors.Error
	}

	tests := []tst{
		{
			name: "minimum valid",
			input: Stream{
				Name: "default",
			},
		},
		{
			name:  "empty valid",
			input: Stream{},
		},
		{
			name: "invalid name too long",
			input: Stream{
				Name: strings.Repeat("a", 512),
			},
			err: errStreamNameTooLong,
		},
		{
			name: "invalid description too long",
			input: Stream{
				Name:        "longTom",
				Description: strings.Repeat("a", 2048),
			},
			err: errStreamDescTooLong,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.err != nil {
				require.Equal(t, test.err, test.input.Validate(false))
			} else {
				require.NoError(t, test.input.Validate(false))
			}
		})
	}
}

func TestBasicStreamIsValid(t *testing.T) {
	type tst struct {
		name     string
		input    BasicStream
		expected bool
	}

	tests := []tst{
		{
			name: "minimum valid",
			input: BasicStream{
				Names: []string{"default"},
			},
			expected: true,
		},
		{
			name:     "invalid",
			input:    BasicStream{},
			expected: false,
		},
		{
			name:     "empty name",
			input:    BasicStream{Names: []string{""}},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.input.IsValid())
		})
	}
}

func TestMashallReadAnnotations(t *testing.T) {
	ra := ReadAnnotations{
		"default": []ReadAnnotation{
			{
				ID:        *annID,
				Summary:   "this is one annotation",
				Stickers:  map[string]string{"env": "testing"},
				StartTime: testTime.Format(time.RFC3339Nano),
				EndTime:   testTime2.Format(time.RFC3339Nano),
			},
			{
				ID:        *annID,
				Summary:   "this is another annotation",
				Stickers:  map[string]string{"env": "testing"},
				StartTime: testTime.Format(time.RFC3339Nano),
				EndTime:   testTime.Format(time.RFC3339Nano),
			},
		},
		"testing": []ReadAnnotation{
			{
				ID:        *annID,
				Summary:   "this is yet another annotation",
				Stickers:  map[string]string{"env": "testing"},
				StartTime: testTime.Format(time.RFC3339Nano),
				EndTime:   testTime.Format(time.RFC3339Nano),
			},
		},
	}

	b, err := json.Marshal(ra)
	require.NoError(t, err)
	require.Greater(t, len(b), 0)
}

func TestSetStickerIncludes(t *testing.T) {
	type tst struct {
		name     string
		input    map[string][]string
		expected AnnotationStickers
	}

	tests := []tst{
		{
			name: "with stickerIncludes",
			input: map[string][]string{
				"stickerIncludes[product]": {"oss"},
				"stickerIncludes[author]":  {"russ"},
				"streams":                  {"default", "blogs"},
			},
			expected: map[string]string{
				"product": "oss",
				"author":  "russ",
			},
		},
		{
			name: "no sticker includes",
			input: map[string][]string{
				"startTime": {"2021-01-13T22%3A17%3A37.953Z"},
				"endTime":   {"2021-01-13T22%3A17%3A37.953Z"},
				"streams":   {"default", "blogs"},
			},
			expected: map[string]string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := AnnotationListFilter{}
			f.SetStickerIncludes(test.input)
			require.Equal(t, test.expected, f.StickerIncludes)
		})
	}
}

func TestSetStickers(t *testing.T) {
	type tst struct {
		name     string
		input    map[string][]string
		expected map[string]string
	}

	tests := []tst{
		{
			name: "with stickers",
			input: map[string][]string{
				"stickers[product]": {"oss"},
				"stickers[author]":  {"russ"},
				"streams":           {"default", "blogs"},
			},
			expected: map[string]string{
				"product": "oss",
				"author":  "russ",
			},
		},
		{
			name: "no stickers",
			input: map[string][]string{
				"startTime": {"2021-01-13T22%3A17%3A37.953Z"},
				"endTime":   {"2021-01-13T22%3A17%3A37.953Z"},
				"streams":   {"default", "blogs"},
			},
			expected: map[string]string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := AnnotationDeleteFilter{}
			f.SetStickers(test.input)
			require.Equal(t, test.expected, f.Stickers)
		})
	}
}

func TestStickerSliceToMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		stickers []string
		want     map[string]string
		wantErr  error
	}{
		{
			"good stickers",
			[]string{"good1=val1", "good2=val2"},
			map[string]string{"good1": "val1", "good2": "val2"},
			nil,
		},
		{
			"bad stickers",
			[]string{"this is an invalid sticker", "shouldbe=likethis"},
			nil,
			invalidStickerError("this is an invalid sticker"),
		},
		{
			"no stickers",
			[]string{},
			map[string]string{},
			nil,
		},
	}

	for _, tt := range tests {
		got, err := stickerSliceToMap(tt.stickers)
		require.Equal(t, tt.want, got)
		require.Equal(t, tt.wantErr, err)
	}
}
