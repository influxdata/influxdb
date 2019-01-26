// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import DataLoadersWizard from 'src/dataLoaders/components/DataLoadersWizard'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'

// Types
import {Substep, DataLoaderType} from 'src/types/v2/dataLoaders'
import {Bucket} from 'src/api'

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
    const {
      buckets,
      type,
      visible,
      onCompleteSetup,
      startingStep,
      startingSubstep,
      startingType,
    } = this.props

    switch (type) {
      case DataLoaderType.LineProtocol:
      case DataLoaderType.Scraping:
      case DataLoaderType.Empty:
        return (
          <DataLoadersWizard
            visible={visible}
            onCompleteSetup={onCompleteSetup}
            buckets={buckets}
            startingStep={startingStep}
            startingSubstep={startingSubstep}
            startingType={startingType}
          />
        )
      case DataLoaderType.Streaming:
        return (
          <CollectorsWizard
            visible={visible}
            onCompleteSetup={onCompleteSetup}
            startingStep={0}
            buckets={buckets}
          />
        )
    }
  }
}

export default DataLoaderSwitcher
