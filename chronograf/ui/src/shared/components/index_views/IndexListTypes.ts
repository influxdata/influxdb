import {Alignment} from 'src/clockface'

export interface IndexListColumn {
  key: string
  title: string
  size: number
  showOnHover: boolean
  align: Alignment
}

export interface IndexListRowColumn {
  key: string
  contents: any
}

export interface IndexListRow {
  columns: IndexListRowColumn[]
  disabled: boolean
}
