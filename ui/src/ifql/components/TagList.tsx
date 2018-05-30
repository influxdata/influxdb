import React, {PureComponent} from 'react'

import {SchemaFilter, Service} from 'src/types'
import TagListItem from 'src/ifql/components/TagListItem'

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
}
