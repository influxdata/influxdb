// Libraries
import React, {FC, PureComponent} from 'react'
import {Switch, Route} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import TelegrafPluginsIndex from 'src/writeData/components/telegrafPlugins/TelegrafPluginsIndex'
import TelegrafPluginsExplainer from 'src/writeData/components/telegrafPlugins/TelegrafPluginsExplainer'
import WriteDataDetailsView from 'src/writeData/components/WriteDataDetailsView'

// Constants
import {ORGS, ORG_ID, TELEGRAF_PLUGINS} from 'src/shared/constants/routes'
import WRITE_DATA_TELEGRAF_PLUGINS_SECTION from 'src/writeData/constants/contentTelegrafPlugins'

const telegrafPluginPath = `/${ORGS}/${ORG_ID}/load-data/${TELEGRAF_PLUGINS}`

const TelegrafPluginsDetailsPage: FC = () => {
  return (
    <WriteDataDetailsView section={WRITE_DATA_TELEGRAF_PLUGINS_SECTION}>
      <TelegrafPluginsExplainer />
    </WriteDataDetailsView>
  )
}

@ErrorHandling
class TelegrafPluginsPage extends PureComponent<{}> {
  public render() {
    const {children} = this.props

    return (
      <>
        <Switch>
          <Route
            path={telegrafPluginPath}
            exact
            component={TelegrafPluginsIndex}
          />
          <Route
            path={`${telegrafPluginPath}/:contentID`}
            component={TelegrafPluginsDetailsPage}
          />
        </Switch>
        {children}
      </>
    )
  }
}

export default TelegrafPluginsPage
