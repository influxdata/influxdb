// Libraries
import React, {PureComponent} from 'react'
import {Switch, Route} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import ClientLibrariesRoot from 'src/clientLibraries/components/ClientLibrariesRoot'
import ArduinoPage from 'src/clientLibraries/components/ClientArduinoPage'
import CSharpPage from 'src/clientLibraries/components/ClientCSharpPage'
import GoPage from 'src/clientLibraries/components/ClientGoPage'
import JavaPage from 'src/clientLibraries/components/ClientJavaPage'
import JSPage from 'src/clientLibraries/components/ClientJSPage'
import KotlinPage from 'src/clientLibraries/components/ClientKotlinPage'
import PHPPage from 'src/clientLibraries/components/ClientPHPPage'
import PythonPage from 'src/clientLibraries/components/ClientPythonPage'
import RubyPage from 'src/clientLibraries/components/ClientRubyPage'
import ScalaPage from 'src/clientLibraries/components/ClientScalaPage'

// Constants
import {ORGS, ORG_ID, CLIENT_LIBS} from 'src/shared/constants/routes'

const clientLibPath = `/${ORGS}/${ORG_ID}/load-data/${CLIENT_LIBS}`

@ErrorHandling
class ClientLibrariesPage extends PureComponent<{}> {
  public render() {
    const {children} = this.props

    return (
      <>
        <Switch>
          <Route path={clientLibPath} exact component={ClientLibrariesRoot} />
          <Route path={`${clientLibPath}/arduino`} component={ArduinoPage} />
          <Route path={`${clientLibPath}/csharp`} component={CSharpPage} />
          <Route path={`${clientLibPath}/go`} component={GoPage} />
          <Route path={`${clientLibPath}/java`} component={JavaPage} />
          <Route path={`${clientLibPath}/javascript-node`} component={JSPage} />
          <Route path={`${clientLibPath}/kotlin`} component={KotlinPage} />
          <Route path={`${clientLibPath}/php`} component={PHPPage} />
          <Route path={`${clientLibPath}/python`} component={PythonPage} />
          <Route path={`${clientLibPath}/ruby`} component={RubyPage} />
          <Route path={`${clientLibPath}/scala`} component={ScalaPage} />
        </Switch>
        {children}
      </>
    )
  }
}

export default ClientLibrariesPage
