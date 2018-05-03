import React, {PureComponent} from 'react'
import _ from 'lodash'

import DatabaseListItem from 'src/ifql/components/DatabaseListItem'

import {Source} from 'src/types'

import {showDatabases} from 'src/shared/apis/metaQuery'
import showDatabasesParser from 'src/shared/parsing/showDatabases'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface DatabaseListProps {
  db: string
  source: Source
  onChooseDatabase: (database: string) => void
}

interface DatabaseListState {
  databases: string[]
}

@ErrorHandling
class DatabaseList extends PureComponent<DatabaseListProps, DatabaseListState> {
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
      const {data} = await showDatabases(source.links.proxy)
      const {databases} = showDatabasesParser(data)
      const dbs = databases.map(database => {
        if (database === '_internal') {
          return `${database}.monitor`
        }

        return `${database}.autogen`
      })

      const sorted = dbs.sort()

      this.setState({databases: sorted})
      const db = _.get(sorted, '0', '')
      this.props.onChooseDatabase(db)
    } catch (err) {
      console.error(err)
    }
  }

  public render() {
    const {onChooseDatabase} = this.props

    return (
      <div className="query-builder--column query-builder--column-db">
        <div className="query-builder--list">
          {this.state.databases.map(db => {
            return (
              <DatabaseListItem
                key={db}
                db={db}
                isActive={this.props.db === db}
                onChooseDatabase={onChooseDatabase}
              />
            )
          })}
        </div>
      </div>
    )
  }
}

export default DatabaseList
