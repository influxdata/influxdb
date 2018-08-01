// Libraries
import React, {PureComponent, MouseEvent} from 'react'

// Components
import TagValueListItem from 'src/flux/components/TagValueListItem'
import LoadingSpinner from 'src/flux/components/LoadingSpinner'

// Types
import {SchemaFilter} from 'src/types'
import {NotificationAction} from 'src/types/notifications'
import {Source} from 'src/types/v2'

interface Props {
  source: Source
  db: string
  tagKey: string
  values: string[]
  filter: SchemaFilter[]
  isLoadingMoreValues: boolean
  onLoadMoreValues: () => void
  shouldShowMoreValues: boolean
  loadMoreCount: number
  notify: NotificationAction
}

export default class TagValueList extends PureComponent<Props> {
  public render() {
    const {
      db,
      notify,
      source,
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
            source={source}
            filter={filter}
            notify={notify}
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
