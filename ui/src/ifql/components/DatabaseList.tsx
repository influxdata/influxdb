import React, {PureComponent} from 'react'
import _ from 'lodash'

import DatabaseListItem from 'src/ifql/components/DatabaseListItem'
import MeasurementList from 'src/ifql/components/MeasurementList'

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
  measurement: string
}

@ErrorHandling
class DatabaseList extends PureComponent<DatabaseListProps, DatabaseListState> {
  constructor(props) {
    super(props)
    this.state = {
      databases: [],
      measurement: '',
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
      const sorted = databases.sort()

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
              <React.Fragment key={db}>
                <DatabaseListItem
                  db={db}
                  isActive={this.props.db === db}
                  onChooseDatabase={onChooseDatabase}
                />
                {this.props.db === db && <MeasurementList db={db} />}
              </React.Fragment>
            )
          })}
        </div>
      </div>
    )
  }
}

export default DatabaseList
