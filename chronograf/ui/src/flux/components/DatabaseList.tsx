// Libraries
import React, {PureComponent} from 'react'

// Components
import DatabaseListItem from 'src/flux/components/DatabaseListItem'

// APIs
import {showDatabases} from 'src/shared/apis/metaQuery'
import showDatabasesParser from 'src/shared/parsing/showDatabases'

// Types
import {Source} from 'src/types'
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
      const {data} = await showDatabases(`${source.links.self}/proxy`)
      const {databases} = showDatabasesParser(data)
      const sorted = databases.sort()

      this.setState({databases: sorted})
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
