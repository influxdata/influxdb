// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LineProtocol from 'src/dataLoaders/components/configureStep/lineProtocol/LineProtocol'
import EmptyDataSourceState from 'src/dataLoaders/components/configureStep/EmptyDataSourceState'
import Scraping from 'src/dataLoaders/components/configureStep/Scraping'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {Bucket} from 'src/api'

export interface Props {
  dataLoaderType: DataLoaderType
  buckets: Bucket[]
  bucket: string
  org: string
  onClickNext: () => void
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    const {bucket, org, dataLoaderType, onClickNext, buckets} = this.props

    switch (dataLoaderType) {
      case DataLoaderType.LineProtocol:
        return (
          <div className="onboarding-step">
            <LineProtocol bucket={bucket} org={org} onClickNext={onClickNext} />
          </div>
        )
      case DataLoaderType.Scraping:
        return (
          <div className="onboarding-step">
            <Scraping onClickNext={onClickNext} buckets={buckets} />
          </div>
        )
      case DataLoaderType.CSV:
        return <div>{DataLoaderType.CSV}</div>
      default:
        return <EmptyDataSourceState />
    }
  }
}

export default ConfigureDataSourceSwitcher
