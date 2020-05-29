// Types
import {ComponentType} from 'react'
import {CancelBox} from 'src/types'
import {State, Dispatch} from 'src/eventViewer/components/EventViewer.reducer'

export interface Row {
  [k: string]: any
}

export type LoadRows = (options: LoadRowsOptions) => CancelBox<Row[]>

export interface LoadRowsOptions {
  offset: number
  limit: number
  until: number
  since?: number
  filter?: SearchExpr
}

export type SearchExpr = SearchBooleanExpr | SearchTagExpr

export interface SearchBooleanExpr {
  type: 'BooleanExpression'
  operator: SearchBooleanOperator
  left: SearchExpr
  right: SearchExpr
}

export interface SearchTagExpr {
  type: 'TagExpression'
  operator: SearchTagOperator
  left: SearchStringLiteral
  right: SearchStringLiteral | SearchRegexLiteral
}

export type SearchBooleanOperator = 'and' | 'or'

export type SearchTagOperator = '==' | '!=' | '=~' | '!~'

export interface SearchStringLiteral {
  type: 'StringLiteral'
  value: string
}

export interface SearchRegexLiteral {
  type: 'RegexLiteral'
  value: string
}

export interface EventViewerChildProps {
  state: State
  dispatch: Dispatch
  loadRows: LoadRows
}

export interface Field {
  rowKey: string
  columnName: string
  columnWidth: number
  component?: ComponentType<{row: Row}>
}

export type Fields = Field[]
