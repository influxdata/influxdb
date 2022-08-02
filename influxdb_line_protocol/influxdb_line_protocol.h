// C API for IOx's Line Protocol parser.

// Validates a line protocol batch.
// Returns NULL on success, otherwise it returns an error message.
// The caller must free the error message with `free_error`.
char *validate_lines(char *lp);

// Free an error message returned by `validate_lines`
void free_error(char *error);
