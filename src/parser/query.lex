%{
#include <stdlib.h>
#include <string.h>
#include "query_types.h"
#include "y.tab.h"

#define YY_USER_ACTION \
  do { \
    yylloc_param->first_line = yylloc_param->last_line;  \
    yylloc_param->first_column = yylloc_param->last_column; \
    yylloc_param->last_line = yylineno;  \
    yylloc_param->last_column += yyleng; \
  } while(0);
%}

static int yycolumn = 1;

%option reentrant
%option debug
%option bison-bridge
%option bison-locations
%option noyywrap
%s FROM_CLAUSE REGEX_CONDITION
%x IN_REGEX
%x IN_TABLE_NAME
%x IN_SIMPLE_NAME
%%

;                         { return *yytext; }
,                         { return *yytext; }
"merge"                   { return MERGE; }
"list"                    { return LIST; }
"series"                  { return SERIES; }
"continuous query"        { return CONTINUOUS_QUERY; }
"continuous queries"      { return CONTINUOUS_QUERIES; }
"inner"                   { return INNER; }
"join"                    { return JOIN; }
"from"                    { BEGIN(FROM_CLAUSE); return FROM; }
<FROM_CLAUSE,REGEX_CONDITION>\/ { BEGIN(IN_REGEX); yylval->string=calloc(1, sizeof(char)); }
<IN_REGEX>\\\/ {
  yylval->string = realloc(yylval->string, strlen(yylval->string) + 2);
  strcat(yylval->string, "/");
}
<IN_REGEX>\\ {
  yylval->string = realloc(yylval->string, strlen(yylval->string) + 2);
  strcat(yylval->string, "\\");
}
<IN_REGEX>\/ {
  BEGIN(INITIAL);
  return REGEX_STRING;
}
<IN_REGEX>\/i {
  BEGIN(INITIAL);
  return INSENSITIVE_REGEX_STRING;
}
<IN_REGEX>[^\\/]* {
  yylval->string=realloc(yylval->string, strlen(yylval->string) + strlen(yytext) + 1);
  strcat(yylval->string, yytext);
}

"where"                   { BEGIN(INITIAL); return WHERE; }
"as"                      { return AS; }
"select"                  { return SELECT; }
"explain"                 { return EXPLAIN; }
"delete"                  { return DELETE; }
"drop series"             { return DROP_SERIES; }
"drop"                    { return DROP; }
"limit"                   { BEGIN(INITIAL); return LIMIT; }
"order"                   { BEGIN(INITIAL); return ORDER; }
"asc"                     { return ASC; }
"in"                      { yylval->string = strdup(yytext); return OPERATION_IN; }
"desc"                    { return DESC; }
"group"                   { BEGIN(INITIAL); return GROUP; }
"by"                      { return BY; }
"into"                    { return INTO; }
"("                       { yylval->character = *yytext; return *yytext; }
")"                       { yylval->character = *yytext; return *yytext; }
"+"                       { yylval->character = *yytext; return *yytext; }
"-"                       { yylval->character = *yytext; return *yytext; }
"*"                       { yylval->character = *yytext; return *yytext; }
"/"                       { yylval->character = *yytext; return *yytext; }
"and"                     { return AND; }
"or"                      { return OR; }
"=~"                      { BEGIN(REGEX_CONDITION); yylval->string = strdup(yytext); return REGEX_OP; }
"="                       { yylval->string = strdup(yytext); return OPERATION_EQUAL; }
"!~"                      { BEGIN(REGEX_CONDITION); yylval->string = strdup(yytext); return NEGATION_REGEX_OP; }
"<>"                      { yylval->string = strdup(yytext); return OPERATION_NE; }
"<"                       { yylval->string = strdup(yytext); return OPERATION_LT; }
">"                       { yylval->string = strdup(yytext); return OPERATION_GT; }
"<="                      { yylval->string = strdup(yytext); return OPERATION_LE; }
">="                      { yylval->string = strdup(yytext); return OPERATION_GE; }

[0-9]+                    { yylval->string = strdup(yytext); return INT_VALUE; }

([0-9]+|[0-9]*\.[0-9]+|[0-9]+\.[0-9]*)([usmhdwy]|ms)      { yylval->string = strdup(yytext); return DURATION; }

[0-9]*\.[0-9]+|[0-9]+\.[0-9]*                       { yylval->string = strdup(yytext); return FLOAT_VALUE; }

true|false                                          { yylval->string = strdup(yytext); return BOOLEAN_VALUE; }

[a-zA-Z0-9_]*                                       { yylval->string = strdup(yytext); return SIMPLE_NAME; }

\" { BEGIN(IN_SIMPLE_NAME); yylval->string=calloc(1, sizeof(char)); }
<IN_SIMPLE_NAME>\\\" {
  yylval->string = realloc(yylval->string, strlen(yylval->string) + 1);
  strcat(yylval->string, "\"");
}
<IN_SIMPLE_NAME>\" {
  BEGIN(INITIAL);
  return SIMPLE_NAME;
}
<IN_SIMPLE_NAME>[^\\"]* {
  yylval->string=realloc(yylval->string, strlen(yylval->string) + strlen(yytext) + 1);
  strcat(yylval->string, yytext);
}

[a-zA-Z0-9_][a-zA-Z0-9._-]*                         { yylval->string = strdup(yytext); return TABLE_NAME; }

\" { BEGIN(IN_TABLE_NAME); yylval->string=calloc(1, sizeof(char)); }
<IN_TABLE_NAME>\\\" {
  yylval->string = realloc(yylval->string, strlen(yylval->string) + 1);
  strcat(yylval->string, "\"");
}
<IN_TABLE_NAME>\" {
  BEGIN(INITIAL);
  return TABLE_NAME;
}
<IN_TABLE_NAME>[^\\"]* {
  yylval->string=realloc(yylval->string, strlen(yylval->string) + strlen(yytext) + 1);
  strcat(yylval->string, yytext);
}

[:\[a-zA-Z0-9_][:\[\]a-zA-Z0-9._-]*                 { yylval->string = strdup(yytext); return INTO_NAME; }

\'[^\']*\'                    {
  yytext[yyleng-1] = '\0';
  yylval->string = strdup(yytext+1);
  return STRING_VALUE;
}

[\t ]*                    {}
.                         { return *yytext; }
