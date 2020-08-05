// Libraries
import React, {PureComponent} from 'react'
import {Switch, Route} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import TelegrafPluginsIndex from 'src/writeData/components/telegrafPlugins/TelegrafPluginsIndex'
import Bcache from 'src/writeData/components//telegrafPlugins/Bcache'

// Constants
import {ORGS, ORG_ID, TELEGRAF_PLUGINS} from 'src/shared/constants/routes'

const telegrafPluginPath = `/${ORGS}/${ORG_ID}/load-data/${TELEGRAF_PLUGINS}`

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
          <Route path={`${telegrafPluginPath}/bcache`} component={Bcache} />
        </Switch>
        {children}
      </>
    )
  }
}

export default TelegrafPluginsPage
