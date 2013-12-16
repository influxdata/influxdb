#include <stdlib.h>
#include "query_types.h"

void
free_array(array *array)
{
  int i;
  for (i = 0; i < array->size; i++)
    free(array->elems[i]);
  free(array->elems);
  free(array);
}
void free_table_name(table_name *name)
{
  free_value(name->name);
  free(name->alias);
  free(name);
}
void
free_table_name_array(table_name_array *array)
{
  int i;
  for (i = 0; i < array->size; i++)
    free_table_name(array->elems[i]);
  free(array->elems);
  free(array);
}

void
free_from_clause(from_clause *f)
{
  free_table_name_array(f->names);
  free(f);
}

void
free_value_array(value_array *array)
{
  int i;
  for (i = 0; i < array->size; i++)
    free_value(array->elems[i]);
  free(array->elems);
  free(array);
}

void
free_groupby_clause(groupby_clause *g)
{
  free_value_array(g->elems);
  if (g->fill_function) {
    free_value(g->fill_function);
  }
  free(g);
}

void
free_value(value *value)
{
  free(value->name);
  if (value->args) free_value_array(value->args);
  free(value);
}

void
free_condition(condition *condition)
{
  if (condition->is_bool_expression) {
    free_value((value*) condition->left);
  } else {
    free_condition(condition->left);
    free_condition(condition->right);
  }
  free(condition);
}

void
free_error (error *error)
{
  free(error->err);
  free(error);
}

void
free_select_query (select_query *q)
{
  if (q->c) {
    // free the columns
    free_value_array(q->c);
  }

  if (q->where_condition) {
    free_condition(q->where_condition);
  }

  if (q->group_by) {
    free_groupby_clause(q->group_by);
  }

  if (q->from_clause) {
    // free the from clause
    free_from_clause(q->from_clause);
  }
}

void
free_delete_query (delete_query *q)
{
  if (q->where_condition) {
    free_condition(q->where_condition);
  }

  if (q->from_clause) {
    // free the from clause
    free_from_clause(q->from_clause);
  }
}

void
close_query (query *q)
{
   if (q->error) {
    free_error(q->error);
   }

  if (q->select_query) {
    free_select_query(q->select_query);
    free(q->select_query);
  }

  if (q->delete_query) {
    free_delete_query(q->delete_query);
    free(q->delete_query);
  }
}
