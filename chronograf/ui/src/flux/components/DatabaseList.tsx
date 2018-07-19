import React, {PureComponent} from 'react'

import {NotificationContext} from 'src/flux/containers/CheckServices'
import DatabaseListItem from 'src/flux/components/DatabaseListItem'

import {showDatabases} from 'src/shared/apis/metaQuery'
import showDatabasesParser from 'src/shared/parsing/showDatabases'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {Service} from 'src/types'

interface Props {
  service: Service
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
    const {service} = this.props

    try {
      const {data} = await showDatabases(`${service.links.source}/proxy`)
      const {databases} = showDatabasesParser(data)
      const sorted = databases.sort()

      this.setState({databases: sorted})
    } catch (err) {
      console.error(err)
    }
  }

  public render() {
    const {databases} = this.state
    const {service} = this.props

    return databases.map(db => {
      return (
        <NotificationContext.Consumer key={db}>
          {({notify}) => (
            <DatabaseListItem db={db} service={service} notify={notify} />
          )}
        </NotificationContext.Consumer>
      )
    })
  }
}

export default DatabaseList
