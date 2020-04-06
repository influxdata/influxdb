import React, {Component} from 'react'
import {Grid} from '@influxdata/clockface'
import GraphTypeSwitcher from './GraphTypeSwitcher'

class PanelSectionBody extends Component {
  render() {
    const {table, status, graphInfo, widths} = this.props

    return (
      <Grid.Column
        widthXS={widths.XS}
        widthSM={widths.SM}
        widthMD={widths.MD}
        key={graphInfo.title}
      >
        <GraphTypeSwitcher
          graphInfo={graphInfo}
          table={table}
          status={status}
        />
      </Grid.Column>
    )
  }
}

export default PanelSectionBody
