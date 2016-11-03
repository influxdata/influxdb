import React, {PropTypes} from 'react';
import selectStatement from '../utils/influxql/select';
import classNames from 'classnames';
import AutoRefresh from 'shared/components/AutoRefresh';
import LineGraph from 'shared/components/LineGraph';
import MultiTable from './MultiTable';
const RefreshingLineGraph = AutoRefresh(LineGraph);

const {bool, shape, string, arrayOf} = PropTypes;
const Visualization = React.createClass({
  propTypes: {
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    queryConfigs: arrayOf(shape({})).isRequired,
    isActive: bool.isRequired,
    name: string,
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
      isGraphInView: true,
    };
  },

  componentDidUpdate() {
    if (this.props.isActive) {
      this.panel.scrollIntoView();
      // scrollIntoView scrolls slightly *too* far, so this adds some top offset.
      this.panel.parentNode.scrollTop -= 10;
    }
  },

  handleToggleView() {
    this.setState({isGraphInView: !this.state.isGraphInView});
  },

  render() {
    const {queryConfigs, timeRange, isActive, name} = this.props;
    const {source} = this.context;
    const proxyLink = source.links.proxy;

    const {isGraphInView} = this.state;
    const statements = queryConfigs.map((query) => {
      const text = query.rawText || selectStatement(timeRange, query);
      return {text, id: query.id};
    });
    const queries = statements.filter((s) => s.text !== null).map((s) => {
      return {host: [proxyLink], text: s.text, id: s.id};
    });
    const autoRefreshMs = 10000;

    return (
      <div ref={(p) => this.panel = p} className={classNames("graph-panel", {active: isActive})}>
        <div className="graph-panel__bar">
          <div className="graph-panel__title">
            {/* <span className="icon caret-right"></span> */}{name || "Graph"}
          </div>
          <div className="graph-panel__left">
            <ul className="graph-panel__tabs">
              <li onClick={this.handleToggleView} className={classNames("graph-panel__tab", {active: isGraphInView})}>Graph</li>
              <li onClick={this.handleToggleView} className={classNames("graph-panel__tab", {active: !isGraphInView})}>Table</li>
            </ul>
          </div>
        </div>
        <div className="graph-panel__graph-container">
          {isGraphInView ? (
            <RefreshingLineGraph
              queries={queries}
              autoRefresh={autoRefreshMs}
              />
          ) : <MultiTable queries={queries} />}
        </div>
      </div>
    );
  },
});

export default Visualization;
