// Libraries
import React, {PureComponent} from 'react'

// Components
import Support from 'src/me/components/Support'
import LogoutButton from 'src/me/components/LogoutButton'
import DashboardsList from 'src/me/components/DashboardsList'
import GetResources from 'src/resources/components/GetResources'
import {
  Panel,
  FlexBox,
  FlexDirection,
  ComponentSize,
  AlignItems,
} from '@influxdata/clockface'
import VersionInfo from 'src/shared/components/VersionInfo'

// Types
import {AppState, ResourceType} from 'src/types'

interface Props {
  me: AppState['me']
}

class ResourceLists extends PureComponent<Props> {
  public render() {
    return (
      <FlexBox
        direction={FlexDirection.Column}
        alignItems={AlignItems.Stretch}
        stretchToFitWidth={true}
        margin={ComponentSize.Small}
      >
        <Panel>
          <Panel.Header>
            <h4>Account</h4>
            <LogoutButton />
          </Panel.Header>
        </Panel>
        <Panel>
          <Panel.Header>
            <h4>Recent Dashboards</h4>
          </Panel.Header>
          <Panel.Body>
            <GetResources resources={[ResourceType.Dashboards]}>
              <DashboardsList />
            </GetResources>
          </Panel.Body>
        </Panel>
        <Panel>
          <Panel.Header>
            <h4>Useful Links</h4>
          </Panel.Header>
          <Panel.Body>
            <Support />
          </Panel.Body>
          <Panel.Footer>
            <VersionInfo />
          </Panel.Footer>
        </Panel>
      </FlexBox>
    )
  }
}

export default ResourceLists
