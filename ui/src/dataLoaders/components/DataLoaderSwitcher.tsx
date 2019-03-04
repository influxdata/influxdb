// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import CreateScraperOverlay from 'src/organizations/components/CreateScraperOverlay'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import LineProtocolWizard from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolWizard'

// Types
import {Substep, DataLoaderType} from 'src/types/v2/dataLoaders'
import {Bucket} from '@influxdata/influx'

interface Props {
  type: DataLoaderType
  onCompleteSetup: () => void
  visible: boolean
  buckets: Bucket[]
  startingType?: DataLoaderType
  startingStep?: number
  startingSubstep?: Substep
}

class DataLoaderSwitcher extends PureComponent<Props> {
  public render() {
    const {buckets, type, visible, onCompleteSetup} = this.props

    switch (type) {
      case DataLoaderType.Empty:
        return <div data-testid="data-loader-empty" />
      case DataLoaderType.Scraping:
        return (
          <CreateScraperOverlay
            visible={visible}
            buckets={buckets}
            onDismiss={onCompleteSetup}
          />
        )
      case DataLoaderType.Streaming:
        return (
          <CollectorsWizard
            visible={visible}
            onCompleteSetup={onCompleteSetup}
            buckets={buckets}
          />
        )
      case DataLoaderType.LineProtocol:
        return (
          <LineProtocolWizard
            onCompleteSetup={onCompleteSetup}
            visible={visible}
            buckets={buckets}
          />
        )
    }
  }
}

export default DataLoaderSwitcher
