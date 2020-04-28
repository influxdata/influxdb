// Libraries
import React, {FunctionComponent, useState} from 'react'
import {connect} from 'react-redux'

// Components
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import WaitingText from 'src/shared/components/WaitingText'
import SelectorList from 'src/timeMachine/components/SelectorList'
import SelectorListCreateBucket from 'src/timeMachine/components/SelectorListCreateBucket'
import {Input} from '@influxdata/clockface'

// Actions
import {selectBucket} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'
import {getAll, getStatus} from 'src/resources/selectors'

// Types
import {AppState, Bucket, ResourceType} from 'src/types'
import {RemoteDataState} from 'src/types'

interface StateProps {
  selectedBucket: string
  bucketNames: string[]
  bucketsStatus: RemoteDataState
}

interface DispatchProps {
  onSelectBucket: (bucket: string, resetSelections: boolean) => void
}

type Props = StateProps & DispatchProps

const fb = term => bucket =>
  bucket.toLocaleLowerCase().includes(term.toLocaleLowerCase())

const BucketSelector: FunctionComponent<Props> = ({
  selectedBucket,
  bucketNames,
  bucketsStatus,
  onSelectBucket,
}) => {
  const [searchTerm, setSearchTerm] = useState('')
  const list = bucketNames.filter(fb(searchTerm))

  const onSelect = (bucket: string) => {
    onSelectBucket(bucket, true)
  }

  if (bucketsStatus === RemoteDataState.Error) {
    return <BuilderCard.Empty>Failed to load buckets</BuilderCard.Empty>
  }

  if (bucketsStatus === RemoteDataState.Loading) {
    return (
      <BuilderCard.Empty>
        <WaitingText text="Loading buckets" />
      </BuilderCard.Empty>
    )
  }

  if (bucketsStatus === RemoteDataState.Done && !bucketNames.length) {
    return <BuilderCard.Empty>No buckets found</BuilderCard.Empty>
  }

  return (
    <>
      <BuilderCard.Menu>
        <Input
          value={searchTerm}
          placeholder="Search for a bucket"
          className="tag-selector--search"
          onChange={e => setSearchTerm(e.target.value)}
        />
      </BuilderCard.Menu>
      <Selector list={list} selected={selectedBucket} onSelect={onSelect} />
    </>
  )
}

interface SelectorProps {
  list: string[]
  selected: string
  onSelect: (bucket: string) => void
}

const Selector: FunctionComponent<SelectorProps> = ({
  list,
  selected,
  onSelect,
}) => {
  if (!list.length) {
    return <BuilderCard.Empty>No buckets matched your search</BuilderCard.Empty>
  }

  return (
    <SelectorList
      items={list}
      selectedItems={[selected]}
      onSelectItem={onSelect}
      multiSelect={false}
      testID="buckets-list"
    >
      <SelectorListCreateBucket />
    </SelectorList>
  )
}

const mstp = (state: AppState) => {
  const buckets = getAll<Bucket>(state, ResourceType.Buckets)
  const bucketNames = buckets.map(bucket => bucket.name || '')
  const bucketsStatus = getStatus(state, ResourceType.Buckets)
  const selectedBucket =
    getActiveQuery(state).builderConfig.buckets[0] || bucketNames[0]

  return {selectedBucket, bucketNames, bucketsStatus}
}

const mdtp = {
  onSelectBucket: selectBucket,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(BucketSelector)
