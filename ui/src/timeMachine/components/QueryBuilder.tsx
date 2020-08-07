// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {range} from 'lodash'

// Components
import TagSelector from 'src/timeMachine/components/TagSelector'
import AggregationSelector from 'src/timeMachine/components/AggregationSelector'
import AddCardButton from 'src/timeMachine/components/AddCardButton'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import BucketsSelector from 'src/timeMachine/components/queryBuilder/BucketsSelector'
import {DapperScrollbars} from '@influxdata/clockface'

// Actions
import {loadBuckets, addTagSelector} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'
import {event} from 'src/cloud/utils/reporting'

// Types
import {AppState} from 'src/types'
import {RemoteDataState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

interface State {}

class TimeMachineQueryBuilder extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    event('TimeMachineQueryBuilder load start')
  }

  public componentDidMount() {
    this.props.onLoadBuckets()
  }

  public render() {
    const {tagFiltersLength} = this.props

    return (
      <div className="query-builder" data-testid="query-builder">
        <div className="query-builder--cards">
          <DapperScrollbars noScrollY={true}>
            <div className="builder-card--list">
              <BuilderCard testID="bucket-selector">
                <BuilderCard.Header title="From" />
                <BucketsSelector />
              </BuilderCard>
              {tagFiltersLength &&
                range(tagFiltersLength).map(i => (
                  <TagSelector key={i} index={i} />
                ))}
              {this.addButton}
            </div>
          </DapperScrollbars>
          {this.functionSelector}
        </div>
      </div>
    )
  }

  private get functionSelector(): JSX.Element {
    const {checkType} = this.props

    if (checkType === 'deadman') {
      return
    }

    return <AggregationSelector />
  }

  private get addButton(): JSX.Element {
    const {moreTags, onAddTagSelector} = this.props

    if (!moreTags) {
      return null
    }

    return <AddCardButton onClick={onAddTagSelector} collapsible={false} />
  }
}

const mstp = (state: AppState) => {
  const tagFiltersLength = getActiveQuery(state).builderConfig.tags.length
  const {
    queryBuilder: {tags},
  } = getActiveTimeMachine(state)

  const {
    alertBuilder: {type: checkType},
  } = state

  if (!tags.length) {
    return {
      tagFiltersLength,
      moreTags: false,
      checkType,
    }
  }

  const {keys, keysStatus} = tags[tags.length - 1]

  return {
    tagFiltersLength,
    moreTags: !(keys.length === 0 && keysStatus === RemoteDataState.Done),
    checkType,
  }
}

const mdtp = {
  onLoadBuckets: loadBuckets,
  onAddTagSelector: addTagSelector,
}

const connector = connect(mstp, mdtp)

export default connector(TimeMachineQueryBuilder)
