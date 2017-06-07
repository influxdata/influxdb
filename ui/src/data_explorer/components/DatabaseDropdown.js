import React, {PropTypes} from 'react'

import {showDatabases, showRetentionPolicies} from 'shared/apis/metaQuery'
import showDatabasesParser from 'shared/parsing/showDatabases'
import showRetentionPoliciesParser from 'shared/parsing/showRetentionPolicies'
import Dropdown from 'shared/components/Dropdown'

const {func, shape, string} = PropTypes

const DatabaseDropdown = React.createClass({
  propTypes: {
    query: shape({}).isRequired,
    onChooseNamespace: func.isRequired,
  },

  contextTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      namespaces: [],
    }
  },

  componentDidMount() {
    const {source} = this.context
    const proxy = source.links.proxy
    showDatabases(proxy).then(resp => {
      const {errors, databases} = showDatabasesParser(resp.data)
      if (errors.length) {
        // do something
      }

      const namespaces = []
      showRetentionPolicies(proxy, databases).then(res => {
        res.data.results.forEach((result, index) => {
          const {errors: errs, retentionPolicies} = showRetentionPoliciesParser(
            result
          )
          if (errs.length) {
            // do something
          }

          retentionPolicies.forEach(rp => {
            namespaces.push({
              database: databases[index],
              retentionPolicy: rp.name,
            })
          })
        })

        this.setState({namespaces})
      })
    })
  },

  render() {
    const {query, onChooseNamespace} = this.props
    const {namespaces} = this.state

    return (
      <Dropdown
        className="dropdown-160 query-builder--db-dropdown"
        items={namespaces.map(n => ({
          ...n,
          text: `${n.database}.${n.retentionPolicy}`,
        }))}
        onChoose={onChooseNamespace}
        selected={
          query.database && query.retentionPolicy
            ? `${query.database}.${query.retentionPolicy}`
            : 'Choose a DB & RP'
        }
      />
    )
  },
})

export default DatabaseDropdown
