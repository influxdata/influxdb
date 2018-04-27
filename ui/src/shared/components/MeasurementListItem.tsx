import _ from 'lodash'
import classnames from 'classnames'
import React, {PureComponent, MouseEvent} from 'react'
import TagList from 'src/shared/components/TagList'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface SourceLinks {
  proxy: string
}

interface Source {
  links: SourceLinks
}

interface GroupBy {
  tags?: string[]
}

interface Tags {
  [key: string]: string[]
}

interface Query {
  database: string
  measurement: string
  retentionPolicy: string
  tags: Tags
  groupBy: GroupBy
}

interface Props {
  query: Query
  querySource: Source
  isActive: boolean
  measurement: string
  numTagsActive: number
  areTagsAccepted: boolean
  onChooseTag: () => void
  onGroupByTag: () => void
  onAcceptReject: () => void
  isQuerySupportedByExplorer: boolean
  onChooseMeasurement: (measurement: string) => () => void
}

interface State {
  isOpen: boolean
}

@ErrorHandling
class MeasurementListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {isOpen: this.isCurrentMeasurement}
  }

  public render() {
    const {
      query,
      querySource,
      measurement,
      onChooseTag,
      onGroupByTag,
      numTagsActive,
      areTagsAccepted,
      isQuerySupportedByExplorer,
    } = this.props

    return (
      <div key={measurement} onClick={this.handleClick}>
        <div
          className={classnames('query-builder--list-item', {
            active: this.shouldShow,
          })}
        >
          <span>
            <div className="query-builder--caret icon caret-right" />
            {measurement}
          </span>
          {this.shouldShow &&
            numTagsActive >= 1 && (
              <div
                className={classnames('flip-toggle', {
                  flipped: areTagsAccepted,
                })}
                onClick={this.handleAcceptReject}
              >
                <div className="flip-toggle--container">
                  <div className="flip-toggle--front">!=</div>
                  <div className="flip-toggle--back">=</div>
                </div>
              </div>
            )}
        </div>
        {this.shouldShow && (
          <TagList
            query={query}
            querySource={querySource}
            onChooseTag={onChooseTag}
            onGroupByTag={onGroupByTag}
            isQuerySupportedByExplorer={isQuerySupportedByExplorer}
          />
        )}
      </div>
    )
  }

  private handleAcceptReject = (e: MouseEvent<HTMLElement>) => {
    e.stopPropagation()

    const {isQuerySupportedByExplorer} = this.props
    if (!isQuerySupportedByExplorer) {
      return
    }

    const {onAcceptReject} = this.props
    onAcceptReject()
  }

  private handleClick = () => {
    const {measurement, onChooseMeasurement} = this.props

    if (!this.isCurrentMeasurement) {
      this.setState({isOpen: true}, () => {
        onChooseMeasurement(measurement)()
      })
    } else {
      this.setState({isOpen: !this.state.isOpen})
    }
  }

  private get shouldShow(): boolean {
    return this.isCurrentMeasurement && this.state.isOpen
  }

  private get isCurrentMeasurement(): boolean {
    const {query, measurement} = this.props
    return _.get(query, 'measurement') === measurement
  }
}

export default MeasurementListItem
