// Libraries
import React, {PureComponent} from 'react'

// Components
import Support from 'src/me/components/Support'
import LogoutButton from 'src/me/components/LogoutButton'
import DashboardsList from 'src/me/components/DashboardsList'
import {
  Panel,
  ComponentSpacer,
  FlexDirection,
  ComponentSize,
  AlignItems,
} from '@influxdata/clockface'
import VersionInfo from 'src/shared/components/VersionInfo'

// Types
import {AppState} from 'src/types'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

interface Props {
  me: AppState['me']
}

class ResourceLists extends PureComponent<Props> {
  public render() {
    return (
      <ComponentSpacer
        direction={FlexDirection.Column}
        alignItems={AlignItems.Stretch}
        stretchToFitWidth={true}
        margin={ComponentSize.Small}
      >
        <Panel>
          <Panel.Header title="Account">
            <LogoutButton />
          </Panel.Header>
        </Panel>
        <Panel>
          <Panel.Header title="Dashboards" />
          <Panel.Body>
            <GetResources resource={ResourceTypes.Dashboards}>
              <DashboardsList />
            </GetResources>
          </Panel.Body>
        </Panel>
        <Panel>
          <Panel.Header title="Useful Links" />
          <Panel.Body>
            <Support />
          </Panel.Body>
          <Panel.Footer>
            <VersionInfo />
          </Panel.Footer>
        </Panel>
      </ComponentSpacer>
    )
  }
}

export default ResourceLists
