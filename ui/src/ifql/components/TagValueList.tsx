import React, {PureComponent} from 'react'

import TagValueListItem from 'src/ifql/components/TagValueListItem'
import {Service, SchemaFilter} from 'src/types'

interface Props {
  service: Service
  db: string
  tag: string
  values: string[]
  filter: SchemaFilter[]
}

export default class TagValueList extends PureComponent<Props> {
  public render() {
    const {db, service, values, tag, filter} = this.props

    return values.map((v, i) => (
      <TagValueListItem
        key={i}
        db={db}
        tag={tag}
        value={v}
        service={service}
        filter={filter}
      />
    ))
  }
}
