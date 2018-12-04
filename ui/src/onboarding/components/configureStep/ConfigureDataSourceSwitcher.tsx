// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

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
