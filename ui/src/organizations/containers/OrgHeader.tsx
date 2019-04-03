import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from 'src/pageLayout'

// Types
import {Organization} from '@influxdata/influx'
import {AppState} from 'src/types'

interface StateProps {
  org: Organization
}

class OrgHeader extends Component<StateProps> {
  public render() {
    const {org} = this.props

    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>
          <Page.Title title={`${org.name} / Settings`} />
        </Page.Header.Left>
        <Page.Header.Right />
      </Page.Header>
    )
  }
}

const mstp = ({orgs: {org}}: AppState) => ({org})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(OrgHeader)
