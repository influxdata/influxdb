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
"merge"                   { return MERGE; }
"inner"                   { return INNER; }
"join"                    { return JOIN; }
"from"                    { return FROM; }
"where"                   { return WHERE; }
"as"                      { return AS; }
"select"                  { return SELECT; }
"limit"                   { return LIMIT; }
"order"                   { return ORDER; }
"asc"                     { return ASC; }
"desc"                    { return DESC; }
"group"                   { return GROUP; }
"by"                      { return BY; }
"("                       { yylval->character = *yytext; return *yytext; }
")"                       { yylval->character = *yytext; return *yytext; }
"+"                       { yylval->character = *yytext; return *yytext; }
"-"                       { yylval->character = *yytext; return *yytext; }
"*"                       { yylval->character = *yytext; return *yytext; }
"/"                       { yylval->character = *yytext; return *yytext; }
"and"                     { return AND; }
"or"                      { return OR; }
"=="                      { yylval->string = strdup(yytext); return OPERATION_EQUAL; }
"=~"                      { yylval->string = strdup(yytext); return REGEX_OP; }
"!~"                      { yylval->string = strdup(yytext); return NEGATION_REGEX_OP; }
"!="                      { yylval->string = strdup(yytext); return OPERATION_NE; }
"<"                       { yylval->string = strdup(yytext); return OPERATION_LT; }
">"                       { yylval->string = strdup(yytext); return OPERATION_GT; }
"<="                      { yylval->string = strdup(yytext); return OPERATION_LE; }
">="                      { yylval->string = strdup(yytext); return OPERATION_GE; }

[0-9]+                    { yylval->string = strdup(yytext); return INT_VALUE; }

[0-9]*\.[0-9]+|[0-9]+\.[0-9]* { yylval->string = strdup(yytext); return FLOAT_VALUE; }

[0-9]+[smhdw]             { yylval->string = strdup(yytext); return DURATION; }

\/.+\/                    { yytext[strlen(yytext)-1]='\0';yylval->string=strdup(yytext+1);return REGEX_STRING; }
\/.+\/i                   { yytext[strlen(yytext)-2]='\0';yylval->string=strdup(yytext+1);return INSENSITIVE_REGEX_STRING; }

[a-zA-Z][a-zA-Z0-9_]*     { yylval->string = strdup(yytext); return SIMPLE_NAME; }

[a-zA-Z][a-zA-Z0-9._-]*   { yylval->string = strdup(yytext); return TABLE_NAME; }

\'.*\'                    {
  yytext[yyleng-1] = '\0';
  yylval->string = strdup(yytext+1);
  return STRING_VALUE;
}
