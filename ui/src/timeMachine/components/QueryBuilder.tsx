// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {range} from 'lodash'

// Components
import {Button, ButtonShape, IconFont} from '@influxdata/clockface'
import {Form} from 'src/clockface'
import TagSelector from 'src/timeMachine/components/TagSelector'
import QueryBuilderBucketDropdown from 'src/timeMachine/components/QueryBuilderBucketDropdown'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import FunctionSelector from 'src/timeMachine/components/FunctionSelector'

// Actions
import {loadBuckets, addTagSelector} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'

// Styles
import 'src/timeMachine/components/QueryBuilder.scss'

// Types
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'

interface StateProps {
  tagFiltersLength: number
  moreTags: boolean
}

interface DispatchProps {
  onLoadBuckets: () => Promise<void>
  onAddTagSelector: () => void
}

type Props = StateProps & DispatchProps

interface State {}

class TimeMachineQueryBuilder extends PureComponent<Props, State> {
  public componentDidMount() {
    this.props.onLoadBuckets()
  }

  public render() {
    const {tagFiltersLength} = this.props

    return (
      <div className="query-builder" data-testid="query-builder">
        <div className="query-builder--buttons">
          <Form.Element label="Bucket">
            <QueryBuilderBucketDropdown />
          </Form.Element>
        </div>
        <div className="query-builder--cards">
          <FancyScrollbar>
            <div className="query-builder--tag-selectors">
              {range(tagFiltersLength).map(i => (
                <TagSelector key={i} index={i} />
              ))}
              {this.addButton}
            </div>
          </FancyScrollbar>
          <FunctionSelector />
        </div>
      </div>
    )
  }

  private get addButton(): JSX.Element {
    const {moreTags, onAddTagSelector} = this.props

    if (!moreTags) {
      return null
    }

    return (
      <Button
        shape={ButtonShape.Square}
        icon={IconFont.Plus}
        onClick={onAddTagSelector}
        customClass="query-builder--add-tag-selector"
      />
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const tagFiltersLength = getActiveQuery(state).builderConfig.tags.length
  const tags = getActiveTimeMachine(state).queryBuilder.tags

  const {keys, keysStatus} = tags[tags.length - 1]

  return {
    tagFiltersLength,
    moreTags: !(keys.length === 0 && keysStatus === RemoteDataState.Done),
  }
}

const mdtp = {
  onLoadBuckets: loadBuckets as any,
  onAddTagSelector: addTagSelector,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueryBuilder)
