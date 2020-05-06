// Libraries
import React, {FC} from 'react'
import {Grid} from '@influxdata/clockface'

// Components
import GraphTypeSwitcher from './GraphTypeSwitcher'

// Types
import {UsageTable, UsageGraphInfo, UsageWidths} from 'src/types'

interface Props {
  table: UsageTable
  graphInfo: UsageGraphInfo
  widths: UsageWidths
}

const PanelSectionBody: FC<Props> = ({table, graphInfo, widths}) => {
  return (
    <Grid.Column
      widthXS={widths.XS}
      widthSM={widths.SM}
      widthMD={widths.MD}
      key={graphInfo.title}
    >
      <GraphTypeSwitcher graphInfo={graphInfo} table={table} />
    </Grid.Column>
  )
}

export default PanelSectionBody
