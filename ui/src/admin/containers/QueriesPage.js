import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux';
import {bindActionCreators} from 'redux';

import flatten from 'lodash/flatten'
import uniqBy from 'lodash/uniqBy'

import {
  showDatabases,
  showQueries,
} from 'shared/apis/metaQuery'

import QueriesTable from 'src/admin/components/QueriesTable'
import showDatabasesParser from 'shared/parsing/showDatabases'
import showQueriesParser from 'shared/parsing/showQueries'
import {TIMES} from 'src/admin/constants'
import {
  loadQueries as loadQueriesAction,
  setQueryToKill as setQueryToKillAction,
  killQueryAsync,
} from 'src/admin/actions'

class QueriesPage extends Component {
  constructor(props) {
    super(props)
    this.updateQueries = this.updateQueries.bind(this)
    this.handleConfirmKillQuery = this.handleConfirmKillQuery.bind(this)
    this.handleKillQuery = this.handleKillQuery.bind(this)
  }

  componentDidMount() {
    this.updateQueries()
    const updateInterval = 5000
    this.intervalID = setInterval(this.updateQueries, updateInterval)
  }

  componentWillUnmount() {
    clearInterval(this.intervalID)
  }

  render() {
    const {queries} = this.props;

    return (
      <div className="page">
        <QueriesHeader />
        <QueriesTable queries={queries} onConfirm={this.handleConfirmKillQuery} onKillQuery={this.handleKillQuery} />
      </div>
    );
  }

  updateQueries() {
    const {source, addFlashMessage, loadQueries} = this.props
    showDatabases(source.links.proxy).then((resp) => {
      const {databases, errors} = showDatabasesParser(resp.data)
      if (errors.length) {
        errors.forEach((message) => addFlashMessage({type: 'error', text: message}))
        return;
      }

      const fetches = databases.map((db) => showQueries(source.links.proxy, db))

      Promise.all(fetches).then((queryResponses) => {
        const allQueries = [];
        queryResponses.forEach((queryResponse) => {
          const result = showQueriesParser(queryResponse.data);
          if (result.errors.length) {
            result.erorrs.forEach((message) => this.props.addFlashMessage({type: 'error', text: message}));
          }

          allQueries.push(...result.queries);
        });

        const queries = uniqBy(flatten(allQueries), (q) => q.id);

        // sorting queries by magnitude, so generally longer queries will appear atop the list
        const sortedQueries = queries.sort((a, b) => {
          const aTime = TIMES.find((t) => a.duration.match(t.test));
          const bTime = TIMES.find((t) => b.duration.match(t.test));
          return +aTime.magnitude <= +bTime.magnitude;
        });

        loadQueries(sortedQueries)
      });
    });
  }

  handleKillQuery(e) {
    e.stopPropagation();
    const id = e.target.dataset.queryId;

    this.props.setQueryToKill(id)
  }

  handleConfirmKillQuery() {
    const {queryIDToKill, source, killQuery} = this.props;
    if (queryIDToKill === null) {
      return;
    }

    killQuery(source.links.proxy, queryIDToKill)
  }
}

const QueriesHeader = () => (
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1>
          Queries
        </h1>
      </div>
    </div>
  </div>
)

const {
  arrayOf,
  func,
  string,
  shape,
} = PropTypes

QueriesPage.propTypes = {
  source: shape({
    links: shape({
      proxy: string,
    }),
  }),
  queries: arrayOf(shape()),
  addFlashMessage: func,
  loadQueries: func,
  queryIDToKill: string,
  setQueryToKill: func,
  killQuery: func,
}

const mapStateToProps = ({admin: {queries, queryIDToKill}}) => ({
  queries,
  queryIDToKill,
})

const mapDispatchToProps = (dispatch) => ({
  loadQueries: bindActionCreators(loadQueriesAction, dispatch),
  setQueryToKill: bindActionCreators(setQueryToKillAction, dispatch),
  killQuery: bindActionCreators(killQueryAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(QueriesPage)
