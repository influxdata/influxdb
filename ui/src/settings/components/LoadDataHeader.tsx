import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'

class LoadDataHeader extends Component {
  public render() {
    return (
      <Page.Header fullWidth={false}>
        <Page.HeaderLeft>
          <PageTitleWithOrg title="Load Data" />
        </Page.HeaderLeft>
        <Page.HeaderRight />
      </Page.Header>
    )
  }
}

export default LoadDataHeader
