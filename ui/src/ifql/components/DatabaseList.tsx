import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'

import DatabaseListItem from 'src/ifql/components/DatabaseListItem'

import {showDatabases} from 'src/shared/apis/metaQuery'
import showDatabasesParser from 'src/shared/parsing/showDatabases'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface DatabaseListState {
  databases: string[]
  measurement: string
  db: string
}

const {shape} = PropTypes

@ErrorHandling
class DatabaseList extends PureComponent<{}, DatabaseListState> {
  public static contextTypes = {
    source: shape({
      links: shape({}).isRequired,
    }).isRequired,
  }

  constructor(props) {
    super(props)
    this.state = {
      databases: [],
      measurement: '',
      db: '',
    }
  }

  public componentDidMount() {
    this.getDatabases()
  }

  public async getDatabases() {
    const {source} = this.context

    try {
      const {data} = await showDatabases(source.links.proxy)
      const {databases} = showDatabasesParser(data)
      const sorted = databases.sort()

      this.setState({databases: sorted})
      const db = _.get(sorted, '0', '')
      this.handleChooseDatabase(db)
    } catch (err) {
      console.error(err)
    }
  }

  public render() {
    return (
      <div className="ifql-schema-tree">
        {this.state.databases.map(db => {
          return (
            <DatabaseListItem
              db={db}
              key={db}
              onChooseDatabase={this.handleChooseDatabase}
            />
          )
        })}
      </div>
    )
  }

  private handleChooseDatabase = (db: string): void => {
    this.setState({db})
  }
}

export default DatabaseList
