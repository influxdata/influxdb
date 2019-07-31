export {Query, Dialect} from 'src/client'

export interface TimeRange {
  lower: string
  upper?: string | null
  seconds?: number
  format?: string
  label?: string
  duration?: string
}
