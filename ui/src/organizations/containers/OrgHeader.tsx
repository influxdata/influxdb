import React, {Component} from 'react'
import {connect} from 'react-redux'

import {Page} from 'src/pageLayout'
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'

import {Organization} from '@influxdata/influx'
import {AppState} from 'src/types/v2'

import {updateOrg} from 'src/organizations/actions/orgs'

interface OwnProps {
  orgID: string
}

interface StateProps {
  org: Organization
}

interface DispatchProps {
  onUpdateOrg: typeof updateOrg
}

type Props = OwnProps & StateProps & DispatchProps

class OrgHeader extends Component<Props> {
  public render() {
    const {org} = this.props

    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>
          <RenamablePageTitle
            name={org.name}
            maxLength={70}
            placeholder="Name this Organization"
            onRename={this.handleUpdateOrg}
          />
        </Page.Header.Left>
        <Page.Header.Right />
      </Page.Header>
    )
  }

  private handleUpdateOrg = (name: string): void => {
    const {org, onUpdateOrg} = this.props

    const updatedOrg = {...org, name}

    onUpdateOrg(updatedOrg)
  }
}

const mstp = (state: AppState, props: OwnProps) => {
  const {orgs} = state
  const org = orgs.find(o => o.id === props.orgID)
  return {
    org,
  }
}

const mdtp: DispatchProps = {
  onUpdateOrg: updateOrg,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(OrgHeader)
