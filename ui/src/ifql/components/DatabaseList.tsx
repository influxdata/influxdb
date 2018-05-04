import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'

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
    } catch (err) {
      console.error(err)
    }
  }

  public render() {
    return this.state.databases.map(db => {
      return <DatabaseListItem db={db} key={db} />
    })
  }
}

export default DatabaseList
