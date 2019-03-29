// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import {range} from 'lodash'

// Components
import {Button, ButtonShape, IconFont} from '@influxdata/clockface'
import TagSelector from 'src/timeMachine/components/TagSelector'
import QueryBuilderDataCard from 'src/timeMachine/components/QueryBuilderDataCard'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import FunctionSelector from 'src/timeMachine/components/FunctionSelector'

// Actions
import {
  loadBuckets,
  addTagSelector as addTagSelectorAction,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'
import {RemoteDataState} from 'src/types'

interface StateProps {
  tagFiltersLength: number
  moreTags: boolean
}

interface DispatchProps {
  onLoadBuckets: typeof loadBuckets
  onAddTagSelector: typeof addTagSelectorAction
}

type Props = StateProps & DispatchProps

class TimeMachineQueryBuilder extends PureComponent<Props & WithRouterProps> {
  public componentDidMount() {
    const {
      params: {orgID},
    } = this.props
    this.props.onLoadBuckets(orgID)
  }

  public render() {
    const {tagFiltersLength} = this.props

    return (
      <div className="query-builder" data-testid="query-builder">
        <div className="query-builder--cards">
          <FancyScrollbar>
            <div className="query-builder--tag-selectors">
              <QueryBuilderDataCard />
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
    const {moreTags} = this.props

    if (!moreTags) {
      return null
    }

    return (
      <Button
        shape={ButtonShape.Square}
        icon={IconFont.Plus}
        onClick={this.handleAddTagSelector}
        customClass="query-builder--add-tag-selector"
      />
    )
  }
  private handleAddTagSelector() {
    const {
      onAddTagSelector,
      params: {orgID},
    } = this.props
    onAddTagSelector(orgID)
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
  onAddTagSelector: addTagSelectorAction,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<Props>(TimeMachineQueryBuilder))
