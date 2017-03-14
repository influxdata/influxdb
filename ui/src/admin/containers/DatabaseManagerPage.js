import React, {PropTypes, Component} from 'react'
import {showDatabases, showRetentionPolicies} from 'src/shared/apis/metaQuery'
import parseShowDatabases from 'src/shared/parsing/showDatabases'
import parseShowRetentionPolicies from 'src/shared/parsing/showRetentionPolicies'
import DatabaseManager from 'src/admin/components/DatabaseManager'

class DatabaseManagerPage extends Component {
  constructor(props) {
    super(props)
    this.state = {
      databases: [],
      retentionPolicies: [],
    }
  }

  async componentDidMount() {
    const {source: {links: {proxy}}} = this.props

    const {data: dbs} = await showDatabases(proxy)
    const {databases} = parseShowDatabases(dbs)

    const {data: {results}} = await showRetentionPolicies(proxy, databases)
    const retentionPolicies = results.map(parseShowRetentionPolicies)

    this.setState({databases, retentionPolicies})
  }

  render() {
    const {databases, retentionPolicies} = this.state
    const rps = retentionPolicies.map((rp) => rp.retentionPolicies)

    if (!databases.length || !retentionPolicies.length) {
      return null
    }

    return <DatabaseManager databases={databases} retentionPolicies={rps} />
  }
}

const {
  shape,
  string,
} = PropTypes

DatabaseManagerPage.propTypes = {
  source: shape({
    links: shape({
      proxy: string,
    }),
  }),
}

export default DatabaseManagerPage
