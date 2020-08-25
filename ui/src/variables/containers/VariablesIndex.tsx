// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {Switch, Route} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Page} from '@influxdata/clockface'
import VariablesTab from 'src/variables/components/VariablesTab'
import GetResources from 'src/resources/components/GetResources'
import CreateVariableOverlay from 'src/variables/components/CreateVariableOverlay'
import RenameVariableOverlay from 'src/variables/components/RenameVariableOverlay'
import UpdateVariableOverlay from 'src/variables/components/UpdateVariableOverlay'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

interface StateProps {
  org: Organization
}

import {ORGS, ORG_ID, SETTINGS, VARIABLES} from 'src/shared/constants/routes'

const varsPath = `/${ORGS}/${ORG_ID}/${SETTINGS}/${VARIABLES}`

@ErrorHandling
class VariablesIndex extends Component<StateProps> {
  public render() {
    const {org} = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Variables', 'Settings'])}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="variables" orgID={org.id}>
            <GetResources resources={[ResourceType.Variables]}>
              <VariablesTab />
            </GetResources>
          </SettingsTabbedPage>
        </Page>
        <Switch>
          <Route path={`${varsPath}/new`} component={CreateVariableOverlay} />
          <Route
            path={`${varsPath}/:id/rename`}
            component={RenameVariableOverlay}
          />
          <Route
            path={`${varsPath}/:id/edit`}
            component={UpdateVariableOverlay}
          />
        </Switch>
      </>
    )
  }
}

const mstp = (state: AppState) => {
  return {org: state.resources.orgs.org}
}

export default connect<StateProps>(mstp)(VariablesIndex)
