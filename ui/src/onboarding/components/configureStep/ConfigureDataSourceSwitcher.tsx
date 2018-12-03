// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {DataSource} from 'src/types/v2/dataLoaders'

export interface Props {
  dataSources: DataSource[]
  currentIndex: number
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    switch (this.configurationStep) {
      case 'CSV':
      case 'Line Protocol':
      default:
        return <div>{this.configurationStep}</div>
    }
  }

  private get configurationStep() {
    const {currentIndex, dataSources} = this.props

    return _.get(
      dataSources,
      `${currentIndex}.name`,
      'Must select a data source'
    )
  }
}

export default ConfigureDataSourceSwitcher
