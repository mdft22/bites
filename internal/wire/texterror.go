package wire

// NewTextError is a client-facing sentence of a type that decodes itself in the core (a decoder
// the framework writes, as opposed to a method of the application's type): it becomes the
// "message" of an entry in the 400 problem+json, with the rule "type" (ADR 0012). The sentence
// must not contain the client's value.
func NewTextError(msg string) *TextError { return &TextError{msg: msg, Rule: RuleType} }
