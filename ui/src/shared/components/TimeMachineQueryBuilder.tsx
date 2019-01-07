// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {range} from 'lodash'

// Components
import {Form, Button, ButtonShape, IconFont} from 'src/clockface'
import TagSelector from 'src/shared/components/TagSelector'
import QueryBuilderBucketDropdown from 'src/shared/components/QueryBuilderBucketDropdown'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import FunctionSelector from 'src/shared/components/FunctionSelector'

// Actions
import {loadBuckets, addTagSelector} from 'src/shared/actions/v2/queryBuilder'

// Utils
import {getActiveQuery} from 'src/shared/selectors/timeMachines'

// Styles
import 'src/shared/components/TimeMachineQueryBuilder.scss'

// Types
import {AppState} from 'src/types/v2'

interface StateProps {
  tagFiltersLength: number
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
    const {tagFiltersLength, onAddTagSelector} = this.props

    return (
      <div className="query-builder">
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
              <Button
                shape={ButtonShape.Square}
                icon={IconFont.Plus}
                onClick={onAddTagSelector}
                customClass="query-builder--add-tag-selector"
              />
            </div>
          </FancyScrollbar>
          <FunctionSelector />
        </div>
      </div>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const tagFiltersLength = getActiveQuery(state).builderConfig.tags.length

  return {tagFiltersLength}
}

const mdtp = {
  onLoadBuckets: loadBuckets as any,
  onAddTagSelector: addTagSelector,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueryBuilder)
