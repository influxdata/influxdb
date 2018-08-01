// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import DatabaseListItem from 'src/flux/components/DatabaseListItem'

// APIs
import {getBuckets} from 'src/shared/apis/v2/buckets'

// Types
import {Source} from 'src/types/v2'
import {NotificationAction} from 'src/types/notifications'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  source: Source
  notify: NotificationAction
}

interface State {
  databases: string[]
}

@ErrorHandling
class DatabaseList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      databases: [],
    }
  }

  public componentDidMount() {
    this.getDatabases()
  }

  public async getDatabases() {
    const {source} = this.props

    try {
      const buckets = await getBuckets(source.links.buckets)
      const sorted = _.sortBy(buckets, b => b.name.toLocaleLowerCase())
      const databases = sorted.map(db => db.name)

      this.setState({databases})
    } catch (err) {
      console.error(err)
    }
  }

  public render() {
    const {databases} = this.state
    const {source, notify} = this.props

    return databases.map(db => {
      return (
        <DatabaseListItem key={db} db={db} source={source} notify={notify} />
      )
    })
  }
}

export default DatabaseList
