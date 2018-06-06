import React, {PureComponent, MouseEvent} from 'react'

import {SchemaFilter, Service} from 'src/types'
import TagListItem from 'src/flux/components/TagListItem'

interface Props {
  db: string
  service: Service
  tags: string[]
  filter: SchemaFilter[]
}

interface State {
  isOpen: boolean
}

export default class TagList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {isOpen: false}
  }

  public render() {
    const {db, service, tags, filter} = this.props

    if (tags.length) {
      return (
        <>
          {tags.map(t => (
            <TagListItem
              key={t}
              db={db}
              tagKey={t}
              service={service}
              filter={filter}
            />
          ))}
        </>
      )
    }

    return (
      <div className="flux-schema-tree flux-schema--child">
        <div className="flux-schema--item no-hover" onClick={this.handleClick}>
          <div className="no-results">No more tag keys.</div>
        </div>
      </div>
    )
  }

  private handleClick(e: MouseEvent<HTMLDivElement>) {
    e.stopPropagation()
  }
}
