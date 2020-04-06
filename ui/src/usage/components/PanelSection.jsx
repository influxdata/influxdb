import React, {Component} from 'react'
import {Panel, Grid} from '@influxdata/clockface'

class PanelSection extends Component {
  render() {
    const {children} = this.props

    return (
      <Panel.Body>
        <Grid.Row>{children}</Grid.Row>
      </Panel.Body>
    )
  }
}

export default PanelSection
