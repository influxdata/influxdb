import React, {PureComponent, MouseEvent} from 'react'
import _ from 'lodash'

import FilterTagValueListItem from 'src/flux/components/FilterTagValueListItem'
import LoadingSpinner from 'src/flux/components/LoadingSpinner'
import {SchemaFilter} from 'src/types'
import {SetFilterTagValue} from 'src/types/flux'

interface Props {
  db: string
  tagKey: string
  values: string[]
  selectedValues: string[]
  onChangeValue: SetFilterTagValue
  filter: SchemaFilter[]
  isLoadingMoreValues: boolean
  onLoadMoreValues: () => void
  shouldShowMoreValues: boolean
  loadMoreCount: number
}

export default class FilterTagValueList extends PureComponent<Props> {
  public render() {
    const {values, tagKey, shouldShowMoreValues} = this.props

    return (
      <>
        {values.map((v, i) => (
          <FilterTagValueListItem
            key={i}
            value={v}
            selected={_.includes(this.props.selectedValues, v)}
            tagKey={tagKey}
            onChangeValue={this.props.onChangeValue}
          />
        ))}
        {shouldShowMoreValues && (
          <div className="flux-schema-tree flux-schema--child">
            <div className="flux-schema--item no-hover">
              <button
                className="btn btn-xs btn-default increase-values-limit"
                onClick={this.handleClick}
              >
                {this.buttonValue}
              </button>
            </div>
          </div>
        )}
      </>
    )
  }

  private handleClick = (e: MouseEvent<HTMLButtonElement>) => {
    e.stopPropagation()
    this.props.onLoadMoreValues()
  }

  private get buttonValue(): string | JSX.Element {
    const {isLoadingMoreValues, loadMoreCount, tagKey} = this.props

    if (isLoadingMoreValues) {
      return <LoadingSpinner />
    }

    return `Load next ${loadMoreCount} values for ${tagKey}`
  }
}
