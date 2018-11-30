// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import DataStreaming from 'src/onboarding/components/configureStep/streaming/DataStreaming'

// Types
import {DataSource} from 'src/types/v2/dataLoaders'

export interface Props {
  dataSources: DataSource[]
  currentIndex: number
  org: string
  username: string
  bucket: string
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    const {org, bucket, username} = this.props

    switch (this.configurationStep) {
      case 'Listening':
        return <DataStreaming org={org} username={username} bucket={bucket} />
      case 'CSV':
      case 'Line Protocol':
      default:
        return <div>{this.configurationStep}</div>
    }
  }

  private get configurationStep() {
    const {currentIndex, dataSources} = this.props

    if (currentIndex === dataSources.length) {
      return 'Listening'
    }

    return dataSources[currentIndex].name
  }
}

export default ConfigureDataSourceSwitcher
