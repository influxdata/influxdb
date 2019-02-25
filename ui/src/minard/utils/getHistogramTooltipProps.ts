import {HistogramTooltipProps, HistogramLayer} from 'src/minard'
import {getBarFill} from 'src/minard/utils/getBarFill'

export const getHistogramTooltipProps = (
  layer: HistogramLayer,
  rowIndices: number[]
): HistogramTooltipProps => {
  const {table, mappings} = layer

  const xMinCol = table.columns.xMin.data
  const xMaxCol = table.columns.xMax.data
  const yMinCol = table.columns.yMin.data
  const yMaxCol = table.columns.yMax.data

  const counts = rowIndices.map(i => {
    const grouping = mappings.fill.reduce(
      (acc, colName) => ({
        ...acc,
        [colName]: table.columns[colName].data[i],
      }),
      {}
    )

    return {
      count: yMaxCol[i] - yMinCol[i],
      color: getBarFill(layer, i),
      grouping,
    }
  })

  const tooltipProps: HistogramTooltipProps = {
    xMin: xMinCol[rowIndices[0]],
    xMax: xMaxCol[rowIndices[0]],
    counts,
  }

  return tooltipProps
}
