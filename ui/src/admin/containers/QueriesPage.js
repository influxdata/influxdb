import React, {PropTypes, Component} from 'react'
import flatten from 'lodash/flatten'
import reject from 'lodash/reject'
import uniqBy from 'lodash/uniqBy'
import {
  showDatabases,
  showQueries,
  killQuery,
} from 'shared/apis/metaQuery'

import showDatabasesParser from 'shared/parsing/showDatabases';
import showQueriesParser from 'shared/parsing/showQueries';
import {TIMES} from 'src/admin/constants'

export default class QueriesPage extends Component {
  constructor(props) {
    super(props)
    this.state = {
      queries: [],
      queryIDToKill: null,
    }

    this.updateQueries = this.updateQueries.bind(this);
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
        <Header />

        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-12">
                <div className="panel panel-minimal">
                  <div className="panel-body">
                    <table className="table v-center">
                      <thead>
                        <tr>
                          <th>Database</th>
                          <th>Query</th>
                          <th>Running</th>
                          <th></th>
                        </tr>
                      </thead>
                      <tbody>
                        {queries.map((q) => {
                          return (
                            <tr key={q.id}>
                              <td>{q.database}</td>
                              <td><code>{q.query}</code></td>
                              <td>{q.duration}</td>
                              <td className="text-right">
                                <button className="btn btn-xs btn-link-danger" onClick={this.handleKillQuery} data-toggle="modal" data-query-id={q.id} data-target="#killModal">
                                  Kill
                                </button>
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className="modal fade" id="killModal" tabIndex="-1" role="dialog" aria-labelledby="myModalLabel">
            <div className="modal-dialog" role="document">
              <div className="modal-content">
                <div className="modal-header">
                  <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                    <span aria-hidden="true">×</span>
                  </button>
                  <h4 className="modal-title" id="myModalLabel">Are you sure you want to kill this query?</h4>
                </div>
                <div className="modal-footer">
                  <button type="button" className="btn btn-default" data-dismiss="modal">No</button>
                  <button type="button" className="btn btn-danger" data-dismiss="modal" onClick={this.handleConfirmKillQuery}>Yes, kill it!</button>
                </div>
              </div>
            </div>
          </div>
        </div>
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

const Header = () => (
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
