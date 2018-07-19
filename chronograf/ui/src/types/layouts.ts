export interface LayoutCell {
  x: number
  y: number
  w: number
  h: number
  i: string
  name: string
  queries: LayoutQuery[]
}

export interface LayoutQuery {
  query: string
  label: string
  wheres: string[]
  groupbys: string[]
}
