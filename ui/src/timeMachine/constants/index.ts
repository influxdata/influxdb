import {ViewType} from 'src/types'

interface VisType {
  type: ViewType
  name: string
}

export const VIS_TYPES: VisType[] = [
  {
    type: 'band',
    name: 'Band Plot',
  },
  {
    type: 'xy',
    name: 'Graph',
  },
  {
    type: 'line-plus-single-stat',
    name: 'Graph + Single Stat',
  },
  {
    type: 'heatmap',
    name: 'Heatmap',
  },
  {
    type: 'mosaic',
    name: 'Mosaic',
  },
  {
    type: 'histogram',
    name: 'Histogram',
  },
  {
    type: 'single-stat',
    name: 'Single Stat',
  },
  {
    type: 'gauge',
    name: 'Gauge',
  },
  {
    type: 'table',
    name: 'Table',
  },
  {
    type: 'scatter',
    name: 'Scatter',
  },
]
