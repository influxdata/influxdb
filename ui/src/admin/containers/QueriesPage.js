import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import flatten from 'lodash/flatten'
import uniqBy from 'lodash/uniqBy'

import {showDatabases, showQueries} from 'shared/apis/metaQuery'

import QueriesTable from 'src/admin/components/QueriesTable'
import showDatabasesParser from 'shared/parsing/showDatabases'
import showQueriesParser from 'shared/parsing/showQueries'
import {TIMES} from 'src/admin/constants'
import {notifyQueriesError} from 'shared/copy/notifications'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {
  loadQueries as loadQueriesAction,
  setQueryToKill as setQueryToKillAction,
  killQueryAsync,
} from 'src/admin/actions/influxdb'

import {notify as notifyAction} from 'shared/actions/notifications'

@ErrorHandling
class QueriesPage extends Component {
  componentDidMount() {
    this.updateQueries()
    const updateInterval = 5000
    this.intervalID = setInterval(this.updateQueries, updateInterval)
  }

  componentWillUnmount() {
    clearInterval(this.intervalID)
  }

  render() {
    const {queries} = this.props

    return <QueriesTable queries={queries} onKillQuery={this.handleKillQuery} />
  }

  updateQueries = () => {
    const {source, notify, loadQueries} = this.props
    showDatabases(source.links.proxy).then(resp => {
      const {databases, errors} = showDatabasesParser(resp.data)
      if (errors.length) {
        errors.forEach(message => notify(notifyQueriesError(message)))
        return
      }

      const fetches = databases.map(db => showQueries(source.links.proxy, db))

      Promise.all(fetches).then(queryResponses => {
        const allQueries = []
        queryResponses.forEach(queryResponse => {
          const result = showQueriesParser(queryResponse.data)
          if (result.errors.length) {
            result.errors.forEach(message =>
              notify(notifyQueriesError(message))
            )
          }

          allQueries.push(...result.queries)
        })

        const queries = uniqBy(flatten(allQueries), q => q.id)

        // sorting queries by magnitude, so generally longer queries will appear atop the list
        const sortedQueries = queries.sort((a, b) => {
          const aTime = TIMES.find(t => a.duration.match(t.test))
          const bTime = TIMES.find(t => b.duration.match(t.test))
          return +aTime.magnitude <= +bTime.magnitude
        })

        loadQueries(sortedQueries)
      })
    })
  }

  handleKillQuery = id => {
    const {source, killQuery} = this.props
    killQuery(source.links.proxy, id)
  }
}

const {arrayOf, func, string, shape} = PropTypes

QueriesPage.propTypes = {
  source: shape({
    links: shape({
      proxy: string,
    }),
  }),
  queries: arrayOf(shape()),
  loadQueries: func,
  queryIDToKill: string,
  setQueryToKill: func,
  killQuery: func,
  notify: func.isRequired,
}

const mapStateToProps = ({adminInfluxDB: {queries, queryIDToKill}}) => ({
  queries,
  queryIDToKill,
})

const mapDispatchToProps = dispatch => ({
  loadQueries: bindActionCreators(loadQueriesAction, dispatch),
  setQueryToKill: bindActionCreators(setQueryToKillAction, dispatch),
  killQuery: bindActionCreators(killQueryAsync, dispatch),
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(QueriesPage)
