import React, {PropTypes, Component} from 'react'

import {showDatabases, showRetentionPolicies} from 'shared/apis/metaQuery'
import showDatabasesParser from 'shared/parsing/showDatabases'
import showRetentionPoliciesParser from 'shared/parsing/showRetentionPolicies'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'

class MultiSelectDBDropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      dbrps: [],
    }

    this._getDbRps = ::this._getDbRps
  }

  componentDidMount() {
    this._getDbRps()
  }

  render() {
    const {dbrps} = this.state
    const {onApply} = this.props

    return <MultiSelectDropdown items={dbrps} onApply={onApply} />
  }

  async _getDbRps() {
    const {source: {links: {proxy}}} = this.context
    const {onErrorThrown} = this.props

    try {
      const {data} = await showDatabases(proxy)
      const {databases, errors} = showDatabasesParser(data)
      if (errors.length > 0) {
        throw errors[0] // only one error can come back from this, but it's returned as an array
      }

      const response = await showRetentionPolicies(proxy, databases)
      const dbrps = response.data.results.reduce((acc, result, i) => {
        const {retentionPolicies} = showRetentionPoliciesParser(result)
        const db = databases[i]

        const rps = retentionPolicies.map(({name: rp}) => ({
          db,
          rp,
          name: `${db}.${rp}`,
        }))

        return [...acc, ...rps]
      }, [])

      this.setState({dbrps})
    } catch (error) {
      console.error(error)
      onErrorThrown(error)
    }
  }
}

const {func, shape, string} = PropTypes

MultiSelectDBDropdown.contextTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }).isRequired,
}

MultiSelectDBDropdown.propTypes = {
  onErrorThrown: func,
  onApply: func.isRequired,
}

export default MultiSelectDBDropdown
