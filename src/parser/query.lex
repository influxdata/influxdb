%{
#include <stdlib.h>
#include <string.h>
#include "query_types.h"
#include "y.tab.h"

#define YY_USER_ACTION \
  do { \
    yylloc->last_line = yylineno;                \
    yylloc_param->last_column = yycolumn+yyleng-1; \
    yycolumn += yyleng; \
  } while(0);
%}

static int yycolumn = 1;

%option reentrant
%option bison-bridge
%option bison-locations
%option noyywrap
%%

;                         { return *yytext; }
,                         { return *yytext; }
first                     { return FIRST; }
last                      { return LAST; }
from                      { return FROM; }
where                     { return WHERE; }
select                    { return SELECT; }
group_by                  { return GROUP_BY; }
"("                       { yylval->character = *yytext; return *yytext; }
")"                       { yylval->character = *yytext; return *yytext; }
"+"                       { yylval->character = *yytext; return *yytext; }
"-"                       { yylval->character = *yytext; return *yytext; }
"*"                       { yylval->character = *yytext; return *yytext; }
"/"                       { yylval->character = *yytext; return *yytext; }
"and"                     { return AND; }
"or"                      { return OR; }
==                        { yylval->string = strdup(yytext); return OPERATION_EQUAL; }
~=                        { yylval->string = strdup(yytext); return REGEX_OP; }
!=                        { yylval->string = strdup(yytext); return OPERATION_NE; }
"<"                       { yylval->string = strdup(yytext); return OPERATION_LT; }
">"                       { yylval->string = strdup(yytext); return OPERATION_GT; }
"<="                      { yylval->string = strdup(yytext); return OPERATION_LE; }
">="                      { yylval->string = strdup(yytext); return OPERATION_GE; }
[0-9]+                    { yylval->string = strdup(yytext); return INT_VALUE; }

\/.*\/i?                    { yylval->string = strdup(yytext); return REGEX_STRING; }

[a-zA-Z*._][a-zA-Z0-9*._]*  { yylval->string = strdup(yytext); return NAME; }
\'.*\'                    {
  yytext[yyleng-1] = '\0';
  yylval->string = strdup(yytext+1);
  return STRING_VALUE;
}
[^ \t',;()<>=!+\-*/]*                        {
  yylval->string = strdup(yytext);
  return STRING_VALUE;
}

