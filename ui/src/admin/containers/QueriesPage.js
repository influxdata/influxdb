import React, {PropTypes, Component} from 'react'
import flatten from 'lodash/flatten'
import reject from 'lodash/reject'
import uniqBy from 'lodash/uniqBy'
import {
  showDatabases,
  showQueries,
  killQuery,
} from 'shared/apis/metaQuery'

import QueriesTable from 'src/admin/components/QueriesTable'
import showDatabasesParser from 'shared/parsing/showDatabases'
import showQueriesParser from 'shared/parsing/showQueries'
import {TIMES} from 'src/admin/constants'

export default class QueriesPage extends Component {
  constructor(props) {
    super(props)
    this.state = {
      queries: [],
      queryIDToKill: null,
    }

    this.updateQueries = this.updateQueries.bind(this);
    this.handleConfirmKillQuery = this.handleConfirmKillQuery.bind(this);
    this.handleKillQuery = this.handleKillQuery.bind(this);
  }

  componentDidMount() {
    this.updateQueries();
    const updateInterval = 5000;
    this.intervalID = setInterval(this.updateQueries, updateInterval);
  }

  componentWillUnmount() {
    clearInterval(this.intervalID);
  }

  updateQueries() {
    const {source, addFlashMessage} = this.props;
    showDatabases(source.links.proxy).then((resp) => {
      const {databases, errors} = showDatabasesParser(resp.data);
      if (errors.length) {
        errors.forEach((message) => addFlashMessage({type: 'error', text: message}));
        return;
      }

      const fetches = databases.map((db) => showQueries(source.links.proxy, db));

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
        this.setState({
          queries: sortedQueries,
        });
      });
    });
  }

  render() {
    const {queries} = this.state;
    return (
      <div className="page">
        <QueriesHeader />
        <QueriesTable queries={queries} onConfirm={this.handleConfirmKillQuery} onKillQuery={this.handleKillQuery} />
      </div>
    );
  }

  handleKillQuery(e) {
    e.stopPropagation();
    const id = e.target.dataset.queryId;
    this.setState({
      queryIDToKill: id,
    });
  }

  handleConfirmKillQuery() {
    const {queryIDToKill} = this.state;
    if (queryIDToKill === null) {
      return;
    }

    // optimitstic update
    const {queries} = this.state;
    this.setState({
      queries: reject(queries, (q) => +q.id === +queryIDToKill),
    });

    // kill the query over http
    const {source} = this.props;
    killQuery(source.links.proxy, queryIDToKill).then(() => {
      this.setState({
        queryIDToKill: null,
      });
    });
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
  addFlashMessage: func,
}
