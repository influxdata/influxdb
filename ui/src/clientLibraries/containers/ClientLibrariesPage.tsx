// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {Switch, Route} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import {Page} from '@influxdata/clockface'
import ClientLibraries from 'src/clientLibraries/components/ClientLibraries'
import ArduinoOverlay from 'src/clientLibraries/components/ClientArduinoOverlay'
import CSharpOverlay from 'src/clientLibraries/components/ClientCSharpOverlay'
import GoOverlay from 'src/clientLibraries/components/ClientGoOverlay'
import JavaOverlay from 'src/clientLibraries/components/ClientJavaOverlay'
import JSOverlay from 'src/clientLibraries/components/ClientJSOverlay'
import KotlinOverlay from 'src/clientLibraries/components/ClientKotlinOverlay'
import PHPOverlay from 'src/clientLibraries/components/ClientPHPOverlay'
import PythonOverlay from 'src/clientLibraries/components/ClientPythonOverlay'
import RubyOverlay from 'src/clientLibraries/components/ClientRubyOverlay'
import ScalaOverlay from 'src/clientLibraries/components/ClientScalaOverlay'

// Types
import {AppState, Organization} from 'src/types'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  org: Organization
}

import {ORGS, ORG_ID, CLIENT_LIBS} from 'src/shared/constants/routes'

const clientLibPath = `/${ORGS}/${ORG_ID}/load-data/${CLIENT_LIBS}`

@ErrorHandling
class ClientLibrariesPage extends PureComponent<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Client Libraries', 'Load Data'])}>
          <LoadDataHeader />
          <LoadDataTabbedPage activeTab="client-libraries" orgID={org.id}>
            <ClientLibraries orgID={org.id} />
          </LoadDataTabbedPage>
        </Page>
        <Switch>
          <Route path={`${clientLibPath}/arduino`} component={ArduinoOverlay} />
          <Route path={`${clientLibPath}/csharp`} component={CSharpOverlay} />
          <Route path={`${clientLibPath}/go`} component={GoOverlay} />
          <Route path={`${clientLibPath}/java`} component={JavaOverlay} />
          <Route
            path={`${clientLibPath}/javascript-node`}
            component={JSOverlay}
          />
          <Route path={`${clientLibPath}/kotlin`} component={KotlinOverlay} />
          <Route path={`${clientLibPath}/php`} component={PHPOverlay} />
          <Route path={`${clientLibPath}/python`} component={PythonOverlay} />
          <Route path={`${clientLibPath}/ruby`} component={RubyOverlay} />
          <Route path={`${clientLibPath}/scala`} component={ScalaOverlay} />
        </Switch>
        {children}
      </>
    )
  }
}

const mstp = (state: AppState) => ({
  org: getOrg(state),
})

export default connect<StateProps>(mstp)(ClientLibrariesPage)
