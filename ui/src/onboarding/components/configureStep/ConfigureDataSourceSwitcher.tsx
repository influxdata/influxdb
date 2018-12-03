// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LineProtocol from 'src/onboarding/components/configureStep/lineProtocol/LineProtocol'

// Types
import {TelegrafPlugin} from 'src/types/v2/dataLoaders'

export interface Props {
  telegrafPlugins: TelegrafPlugin[]
  currentIndex: number
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    switch (this.configurationStep) {
      case 'Streaming':
        return <div />
      case 'CSV':
      case 'Line Protocol':
        return <LineProtocol />
      default:
        return <div>{this.configurationStep}</div>
    }
  }

  private get configurationStep() {
    const {currentIndex, telegrafPlugins} = this.props

    return _.get(
      telegrafPlugins,
      `${currentIndex}.name`,
      'Must select a data source'
    )
  }
}

export default ConfigureDataSourceSwitcher
