// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LineProtocol from 'src/onboarding/components/configureStep/lineProtocol/LineProtocol'

// Types
import {TelegrafPlugin, DataLoaderType} from 'src/types/v2/dataLoaders'

export interface Props {
  telegrafPlugins: TelegrafPlugin[]
  currentIndex: number
  dataLoaderType: DataLoaderType
  bucket: string
  org: string
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    const {bucket, org} = this.props
    switch (this.configurationStep) {
      case DataLoaderType.LineProtocol:
        return <LineProtocol bucket={bucket} org={org} />
      case DataLoaderType.CSV:
        return <div>{DataLoaderType.CSV}</div>
      default:
        return <div>{this.configurationStep}</div>
    }
  }

  private get configurationStep() {
    const {currentIndex, telegrafPlugins, dataLoaderType} = this.props
    if (dataLoaderType === DataLoaderType.Streaming) {
      return _.get(
        telegrafPlugins,
        `${currentIndex}.name`,
        'Must select a data source'
      )
    }
    return dataLoaderType
  }
}

export default ConfigureDataSourceSwitcher
