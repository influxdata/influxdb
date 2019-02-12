import {HistogramTooltipProps, Layer} from 'src/minard'
import {getBarFill} from 'src/minard/utils/getBarFill'

export const getHistogramTooltipProps = (
  layer: Layer,
  rowIndices: number[]
): HistogramTooltipProps => {
  const {table, aesthetics} = layer
  const xMinCol = table.columns[aesthetics.xMin]
  const xMaxCol = table.columns[aesthetics.xMax]
  const yMinCol = table.columns[aesthetics.yMin]
  const yMaxCol = table.columns[aesthetics.yMax]

  const counts = rowIndices.map(i => {
    const grouping = aesthetics.fill.reduce(
      (acc, colName) => ({
        ...acc,
        [colName]: table.columns[colName][i],
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
