// -*- go -*-

// definitions

%{
	import (
		"fmt"
	)
%}


// rules
%union
{
	node Node
}
%type <node> hello
%%

hello: 'hello'
{
	// whatever
}

// subroutines
%%
type Lexer int

func (self *Lexer) Lex(yylval *query_SymType) int {
	fmt.Printf("got %v\n", yylval)
	return 0;
}

func (self *Lexer) Error(e error) {
	fmt.Printf("Error: %s\n", err)
}
