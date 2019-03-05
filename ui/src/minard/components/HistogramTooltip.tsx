import {SFC} from 'react'

import {HistogramTooltipProps, HistogramLayer} from 'src/minard'
import {getHistogramTooltipProps} from 'src/minard/utils/getHistogramTooltipProps'

interface Props {
  tooltip?: (props: HistogramTooltipProps) => JSX.Element
  layer: HistogramLayer
  hoveredRowIndices: number[] | null
}

const HistogramTooltip: SFC<Props> = ({
  tooltip,
  layer,
  hoveredRowIndices,
}: Props) => {
  if (!hoveredRowIndices || !tooltip) {
    return null
  }

  return tooltip(getHistogramTooltipProps(layer, hoveredRowIndices))
}

export default HistogramTooltip
