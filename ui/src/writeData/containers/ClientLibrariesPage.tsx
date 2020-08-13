// Libraries
import React, {FC, PureComponent} from 'react'
import {Switch, Route} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import ClientLibrariesIndex from 'src/writeData/components/clientLibraries/ClientLibrariesIndex'
import WriteDataDetailsView from 'src/writeData/components/WriteDataDetailsView'
import WriteDataHelper from 'src/writeData/components/WriteDataHelper'

// Constants
import {ORGS, ORG_ID, CLIENT_LIBS} from 'src/shared/constants/routes'
import WRITE_DATA_CLIENT_LIBRARIES_SECTION from 'src/writeData/constants/contentClientLibraries'

const clientLibPath = `/${ORGS}/${ORG_ID}/load-data/${CLIENT_LIBS}`

const ClientLibrariesDetailsPage: FC = () => {
  return (
    <WriteDataDetailsView section={WRITE_DATA_CLIENT_LIBRARIES_SECTION}>
      <WriteDataHelper />
    </WriteDataDetailsView>
  )
}

@ErrorHandling
class ClientLibrariesPage extends PureComponent<{}> {
  public render() {
    const {children} = this.props

    return (
      <>
        <Switch>
          <Route path={clientLibPath} exact component={ClientLibrariesIndex} />
          <Route
            path={`${clientLibPath}/:contentID`}
            component={ClientLibrariesDetailsPage}
          />
        </Switch>
        {children}
      </>
    )
  }
}

export default ClientLibrariesPage
