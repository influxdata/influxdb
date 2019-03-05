export type Node =
  | Package
  | File
  | PackageClause
  | ImportDeclaration
  | Block
  | BadStatement
  | ExpressionStatement
  | ReturnStatement
  | OptionStatement
  | BuiltinStatement
  | VariableAssignment
  | MemberAssignment
  | ArrayExpression
  | FunctionExpression
  | BinaryExpression
  | CallExpression
  | ConditionalExpression
  | LogicalExpression
  | MemberExpression
  | IndexExpression
  | PipeExpression
  | ObjectExpression
  | UnaryExpression
  | Property
  | Identifier
  | BooleanLiteral
  | DateTimeLiteral
  | DurationLiteral
  | FloatLiteral
  | IntegerLiteral
  | PipeLiteral
  | RegexpLiteral
  | StringLiteral
  | UnsignedIntegerLiteral

export interface Position {
  line: number
  column: number
}

export interface SourceLocation {
  file: string
  start: Position
  end: Position
  source: string
}

export interface BaseNode {
  location?: SourceLocation
  errors?: Array<{msg: string}>
}

export interface Package extends BaseNode {
  path?: string
  package: string
  files: File[]
  type: 'Package'
}

export interface File extends BaseNode {
  name?: string
  package: PackageClause
  imports: ImportDeclaration[]
  body: Statement[]
  type: 'File'
}

export interface PackageClause extends BaseNode {
  name: Identifier
  type: 'PackageClause'
}

export interface ImportDeclaration extends BaseNode {
  as: Identifier
  path: StringLiteral
  type: 'ImportDeclaration'
}

export interface Block extends BaseNode {
  body: Statement[]
  type: 'Block'
}

export type Statement =
  | BadStatement
  | ExpressionStatement
  | ReturnStatement
  | OptionStatement
  | BuiltinStatement
  | VariableAssignment
  | MemberAssignment

export interface BadStatement extends BaseNode {
  text: string
  type: 'BadStatement'
}

export interface ExpressionStatement extends BaseNode {
  expression: Expression
  type: 'ExpressionStatement'
}

export interface ReturnStatement extends BaseNode {
  argument: Expression
  type: 'ReturnStatement'
}

export interface OptionStatement extends BaseNode {
  assignment: Assignment
  type: 'OptionStatement'
}

export interface BuiltinStatement extends BaseNode {
  id: Identifier
  type: 'BuiltinStatement'
}

export type Assignment = VariableAssignment | MemberAssignment

export interface VariableAssignment extends BaseNode {
  id: Identifier
  init: Expression
  type: 'VariableAssignment'
}

export interface MemberAssignment extends BaseNode {
  member: MemberExpression
  init: Expression
  type: 'MemberExpression'
}

export type Expression =
  | ArrayExpression
  | FunctionExpression
  | BinaryExpression
  | BooleanLiteral
  | CallExpression
  | ConditionalExpression
  | DateTimeLiteral
  | DurationLiteral
  | FloatLiteral
  | Identifier
  | IntegerLiteral
  | LogicalExpression
  | MemberExpression
  | IndexExpression
  | ObjectExpression
  | PipeExpression
  | PipeLiteral
  | RegexpLiteral
  | StringLiteral
  | UnaryExpression
  | UnsignedIntegerLiteral

export interface ArrayExpression extends BaseNode {
  elements: Expression[]
  type: 'ArrayExpression'
}

export interface FunctionExpression extends BaseNode {
  params: Property[]
  body: Node
  type: 'FunctionExpression'
}

export interface BinaryExpression extends BaseNode {
  operator: Operator
  left: Expression
  right: Expression
  type: 'BinaryExpression'
}

export interface CallExpression extends BaseNode {
  callee: Expression
  arguments?: Expression[]
  type: 'CallExpression'
}

export interface ConditionalExpression extends BaseNode {
  test: Expression
  alternate: Expression
  consequent: Expression
  type: 'ConditionalExpression'
}

export interface LogicalExpression extends BaseNode {
  operator: LogicalOperator
  left: Expression
  right: Expression
  type: 'LogicalExpression'
}

export interface MemberExpression extends BaseNode {
  object: Expression
  property: PropertyKey
  type: 'MemberExpression'
}

export interface IndexExpression extends BaseNode {
  array: Expression
  index: Expression
  type: 'IndexExpression'
}

export interface PipeExpression extends BaseNode {
  argument: Expression
  call: CallExpression
  type: 'PipeExpression'
}

export interface ObjectExpression extends BaseNode {
  properties: Property[]
  type: 'ObjectExpression'
}

export interface UnaryExpression extends BaseNode {
  operator: Operator
  argument: Expression
  type: 'UnaryExpression'
}

export interface Property extends BaseNode {
  key: PropertyKey
  value: Expression
  type: 'Property'
}

export interface Identifier extends BaseNode {
  name: string
  type: 'Identifier'
}

export interface BooleanLiteral extends BaseNode {
  value: boolean
  type: 'BooleanLiteral'
}

export interface DateTimeLiteral extends BaseNode {
  value: string // RCF3339 datetime
  type: 'DateTimeLiteral'
}

export type DurationUnit =
  | 'y'
  | 'mo'
  | 'w'
  | 'd'
  | 'h'
  | 'm'
  | 's'
  | 'ms'
  | 'us'
  | 'µs'
  | 'ns'

export interface Duration {
  magnitude: number
  unit: DurationUnit
}

export interface DurationLiteral extends BaseNode {
  values: Duration[]
  type: 'DurationLiteral'
}

export interface FloatLiteral extends BaseNode {
  value: number
  type: 'FloatLiteral'
}

export interface IntegerLiteral extends BaseNode {
  value: number
  type: 'IntegerLiteral'
}

export interface PipeLiteral extends BaseNode {
  type: 'PipeLiteral'
}

export interface RegexpLiteral extends BaseNode {
  value: string
  type: 'RegexpLiteral'
}

export interface StringLiteral extends BaseNode {
  value: string
  type: 'StringLiteral'
}

export interface UnsignedIntegerLiteral extends BaseNode {
  value: number
  type: 'UnsignedIntegerLiteral'
}

export type MultiplicationOperator = '*'
export type DivisionOperator = '/'
export type AdditionOperator = '+'
export type SubtractionOperator = '-'
export type LessThanEqualOperator = '<='
export type LessThanOperator = '<'
export type GreaterThanOperator = '>'
export type GreaterThanEqualOperator = '>='
export type InOperator = 'in'
export type NotOperator = 'not'
export type NotEmptyOperator = 'not empty'
export type EmptyOperator = 'empty'
export type StartsWithOperator = 'startswith'
export type EqualOperator = '=='
export type NotEqualOperator = '!='
export type RegexpMatchOperator = '=~'
export type NotRegexpMatchOperator = '!~'
export type AndOperator = 'and'
export type OrOperator = 'or'

export type Operator =
  | MultiplicationOperator
  | DivisionOperator
  | AdditionOperator
  | SubtractionOperator
  | LessThanEqualOperator
  | LessThanOperator
  | GreaterThanOperator
  | GreaterThanEqualOperator
  | InOperator
  | NotOperator
  | NotEmptyOperator
  | EmptyOperator
  | StartsWithOperator
  | EqualOperator
  | NotEqualOperator
  | RegexpMatchOperator
  | NotRegexpMatchOperator

export type LogicalOperator = AndOperator | OrOperator

export type PropertyKey = Identifier | StringLiteral
