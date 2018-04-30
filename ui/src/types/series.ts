export type TimeSeriesValue = string | number | Date | null

export interface Series {
  name: string
  columns: string[]
  values: TimeSeriesValue[]
}

export interface Result {
  series: Series[]
  statement_id: number
}

export interface TimeSeriesResponse {
  results: Result[]
}

export interface TimeSeriesServerResponse {
  response: TimeSeriesResponse
}
