import React, {PureComponent, MouseEvent} from 'react'

import TagValueListItem from 'src/ifql/components/TagValueListItem'
import LoadingSpinner from 'src/ifql/components/LoadingSpinner'
import {Service, SchemaFilter} from 'src/types'

interface Props {
  service: Service
  db: string
  tagKey: string
  values: string[]
  filter: SchemaFilter[]
  isLoadingMoreValues: boolean
  onLoadMoreValues: () => void
  shouldShowMoreValues: boolean
  loadMoreCount: number
}

export default class TagValueList extends PureComponent<Props> {
  public render() {
    const {
      db,
      service,
      values,
      tagKey,
      filter,
      shouldShowMoreValues,
    } = this.props

    return (
      <>
        {values.map((v, i) => (
          <TagValueListItem
            key={i}
            db={db}
            value={v}
            tagKey={tagKey}
            service={service}
            filter={filter}
          />
        ))}
        {shouldShowMoreValues && (
          <div className="ifql-schema-tree ifql-tree-node">
            <div className="ifql-schema-item no-hover">
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
