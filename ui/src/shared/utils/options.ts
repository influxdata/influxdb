import {Package, File, Expression, ObjectExpression} from 'src/types/ast'

/*
  Helper functions for editing option statements and object expressions
  from the AST of a flux program.

    option v = {a: 5, b: 6} // declaring an option in flux

  After parsing a flux program via wasm and getting an AST representation,
  it is sometimes useful/necessary to edit the options that are defined.
  The functions in this file provide an interface for getting, setting,
  and deleting options from the AST. The same interface is also provided
  for the properties of an object expression.
*/

export function getProperty(r: ObjectExpression, label: string): Expression {
  for (const prop of r.properties) {
    if (
      prop.key.type === 'Identifier' &&
      'name' in prop.key &&
      prop.key.name === label
    ) {
      return prop.value
    }
    if (
      prop.key.type === 'StringLiteral' &&
      'value' in prop.key &&
      prop.key.value === label
    ) {
      return prop.value
    }
  }
}

export function setProperty(
  r: ObjectExpression,
  label: string,
  expr: Expression
): void {
  for (const prop of r.properties) {
    if (
      prop.key.type === 'Identifier' &&
      'name' in prop.key &&
      prop.key.name === label
    ) {
      prop.value = expr
      return
    }
    if (
      prop.key.type === 'StringLiteral' &&
      'value' in prop.key &&
      prop.key.value === label
    ) {
      prop.value = expr
      return
    }
  }
  r.properties.push({
    type: 'Property' as const,
    key: {type: 'Identifier' as const, name: label},
    value: expr,
  })
}

export function deleteProperty(r: ObjectExpression, label: string): void {
  r.properties = r.properties.filter(prop => {
    if (
      prop.key.type === 'Identifier' &&
      'name' in prop.key &&
      prop.key.name === label
    ) {
      return false
    }
    if (
      prop.key.type === 'StringLiteral' &&
      'value' in prop.key &&
      prop.key.value === label
    ) {
      return false
    }
    return true
  })
}

export function optionFromFile(f: File, name: string): Expression {
  for (const stmt of f.body) {
    if (
      stmt.type === 'OptionStatement' &&
      'assignment' in stmt &&
      'id' in stmt.assignment &&
      stmt.assignment.id.name === name
    ) {
      return stmt.assignment.init
    }
  }
}

export function optionFromPackage(p: Package, name: string): Expression {
  for (const file of p.files) {
    const expr = optionFromFile(file, name)
    if (expr) {
      return expr
    }
  }
}

export function setOption(f: File, name: string, expr: Expression): void {
  for (const stmt of f.body) {
    if (
      stmt.type === 'OptionStatement' &&
      'assignment' in stmt &&
      'id' in stmt.assignment &&
      stmt.assignment.id.name === name
    ) {
      stmt.assignment.init = expr
      return
    }
  }
  f.body.unshift({
    type: 'OptionStatement' as const,
    assignment: {
      type: 'VariableAssignment' as const,
      id: {type: 'Identifier' as const, name: name},
      init: expr,
    },
  })
}

export function deleteOption(f: File, name: string): void {
  f.body = f.body.filter(stmt => {
    if (
      stmt.type === 'OptionStatement' &&
      'assignment' in stmt &&
      'id' in stmt.assignment
    ) {
      return stmt.assignment.id.name !== name
    }
    return true
  })
}
