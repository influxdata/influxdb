import {Expression} from 'src/types/ast'

export const formatExpression = (expr: Expression): string => {
  switch (expr.type) {
    case 'DateTimeLiteral':
    case 'BooleanLiteral':
    case 'UnsignedIntegerLiteral':
    case 'IntegerLiteral':
      return String(expr.value)
    case 'StringLiteral':
      return `"${expr.value}"`
    case 'DurationLiteral':
      return expr.values.reduce(
        (acc, {magnitude, unit}) => `${acc}${magnitude}${unit}`,
        ''
      )
    case 'FloatLiteral':
      return String(expr.value).includes('.')
        ? String(expr.value)
        : expr.value.toFixed(1)
    case 'UnaryExpression':
      return `${expr.operator}${formatExpression(expr.argument)}`
    case 'BinaryExpression':
      return `${formatExpression(expr.left)} ${
        expr.operator
      } ${formatExpression(expr.right)}`
    case 'CallExpression':
      // This doesn't handle formatting a call expression with arguments, or
      // with any other sort of callee except an `Identifier`
      return `${formatExpression(expr.callee)}()`
    case 'Identifier':
      return expr.name
    default:
      throw new Error(`cant format expression of type ${expr.type}`)
  }
}
