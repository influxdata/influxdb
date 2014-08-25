package schema

type FieldLookupError struct {
	message string
}

func NewFieldLookupError(message string) *FieldLookupError {
	return &FieldLookupError{message}
}

func (self FieldLookupError) Error() string {
	return self.message
}
