import {HistogramTooltipProps, Layer} from 'src/minard'
import {getBarFill} from 'src/minard/utils/getBarFill'

export const getHistogramTooltipProps = (
  layer: Layer,
  rowIndices: number[]
): HistogramTooltipProps => {
  const {table, mappings} = layer
  const xMinCol = table.columns[mappings.xMin]
  const xMaxCol = table.columns[mappings.xMax]
  const yMinCol = table.columns[mappings.yMin]
  const yMaxCol = table.columns[mappings.yMax]

  const counts = rowIndices.map(i => {
    const grouping = mappings.fill.reduce(
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
