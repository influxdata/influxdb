import * as React from 'react'
import {FunctionComponent} from 'react'
import AutoSizer from 'react-virtualized-auto-sizer'

import {Config, SizedConfig} from '../types'
import {SizedPlot} from './SizedPlot'

interface Props {
  config: Config
}

export const Plot: FunctionComponent<Props> = ({config, children}) => {
  if (config.width && config.height) {
    return <SizedPlot config={config as SizedConfig}>{children}</SizedPlot>
  }

  return (
    <AutoSizer className="vis-autosizer">
      {({width, height}) => {
        if (width === 0 || height === 0) {
          return null
        }

        return (
          <SizedPlot config={{...config, width, height}}>{children}</SizedPlot>
        )
      }}
    </AutoSizer>
  )
}
