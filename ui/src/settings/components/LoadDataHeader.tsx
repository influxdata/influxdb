import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'

class LoadDataHeader extends Component {
  public render() {
    return (
      <Page.ControlBar fullWidth={false}>
        <Page.ControlBarLeft>
          <PageTitleWithOrg title="Load Data" />
        </Page.ControlBarLeft>
        <Page.ControlBarRight />
      </Page.ControlBar>
    )
  }
}

export default LoadDataHeader
