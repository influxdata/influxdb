// Libraries
import React, {PureComponent} from 'react'
import {Switch, Route} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import ClientLibrariesIndex from 'src/writeData/components/clientLibraries/ClientLibrariesIndex'
import ArduinoPage from 'src/writeData/components/clientLibraries/ArduinoPage'
import CSharpPage from 'src/writeData/components/clientLibraries/CSharpPage'
import GoPage from 'src/writeData/components/clientLibraries/GoPage'
import JavaPage from 'src/writeData/components/clientLibraries/JavaPage'
import JSPage from 'src/writeData/components/clientLibraries/JSPage'
import KotlinPage from 'src/writeData/components/clientLibraries/KotlinPage'
import PHPPage from 'src/writeData/components/clientLibraries/PHPPage'
import PythonPage from 'src/writeData/components/clientLibraries/PythonPage'
import RubyPage from 'src/writeData/components/clientLibraries/RubyPage'
import ScalaPage from 'src/writeData/components/clientLibraries/ScalaPage'

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
          <Route path={clientLibPath} exact component={ClientLibrariesIndex} />
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
