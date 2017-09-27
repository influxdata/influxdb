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
  }

  componentDidMount() {
    this._getDbRps()
  }

  render() {
    const {dbrps} = this.state
    const {onApply, selectedItems} = this.props
    const label = 'Select databases'

    return (
      <MultiSelectDropdown
        label={label}
        items={dbrps}
        onApply={onApply}
        isApplyShown={false}
        selectedItems={selectedItems}
      />
    )
  }

  _getDbRps = async () => {
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

const {arrayOf, func, shape, string} = PropTypes

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
  selectedItems: arrayOf(shape()),
}

export default MultiSelectDBDropdown
