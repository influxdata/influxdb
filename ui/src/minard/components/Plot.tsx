import React, {SFC} from 'react'
import {AutoSizer} from 'react-virtualized'

import {
  SizedPlot,
  Props as SizedPlotProps,
} from 'src/minard/components/SizedPlot'

type Props = Pick<
  SizedPlotProps,
  Exclude<keyof SizedPlotProps, 'width' | 'height'>
> & {width?: number; height?: number}

/*
  Works just like a `SizedPlot`, except it will measure the width and height of
  the containing element if no `width` and `height` props are passed.
*/
export const Plot: SFC<Props> = props => {
  if (props.width && props.height) {
    return <SizedPlot {...props} width={props.width} height={props.height} />
  }

  return (
    <AutoSizer>
      {({width, height}) => {
        if (width === 0 || height === 0) {
          return null
        }

        return <SizedPlot {...props} width={width} height={height} />
      }}
    </AutoSizer>
  )
}
