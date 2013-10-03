%{
#include <stdlib.h>
#include <string.h>
#include "query_types.h"
#include "y.tab.h"
%}

%option reentrant
%option bison-bridge
%option noyywrap
%%

;                         { return *yytext; }
from                      { return FROM; }
where                     { return WHERE; }
select                    { return SELECT; }
=                         { return *yytext; }
[a-zA-Z][a-zA-Z0-9]*      { yylval->string = strdup(yytext); return NAME; }
[0-9]+                    { yylval->i = atoi(yytext); return INT_VALUE; }
\'.*\'                    {
  yytext[yyleng-1] = '\0';
  yylval->string = strdup(yytext+1);
  return STRING_VALUE;
}
