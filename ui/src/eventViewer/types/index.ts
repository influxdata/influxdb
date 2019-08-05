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
  since: number
  until?: number
  filter?: SearchExpr
}

export type SearchExpr = SearchNotExpr | SearchBinaryExpr | string | number

export interface SearchNotExpr {
  type: 'NOT'
  value: SearchExpr
}

export interface SearchBinaryExpr {
  type: 'BINARY_EXPR'
  left: SearchExpr
  right: SearchExpr
  op: SearchBinaryOp
}

export type SearchBinaryOp =
  | 'AND'
  | 'OR'
  | 'EQUALS'
  | 'NOT_EQUALS'
  | 'REG_MATCH'
  | 'NOT_REG_MATCH'

export interface EventViewerChildProps {
  state: State
  dispatch: Dispatch
  loadRows: LoadRows
}

export interface FieldComponents {
  [fieldName: string]: ComponentType<{row: Row}>
}
