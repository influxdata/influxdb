import React, {PropTypes} from 'react';
import selectStatement from '../utils/influxql/select';
import classNames from 'classnames';
import AutoRefresh from 'shared/components/AutoRefresh';
import LineGraph from 'shared/components/LineGraph';
import MultiTable from './MultiTable';
const RefreshingLineGraph = AutoRefresh(LineGraph);

const {
  arrayOf,
  number,
  shape,
  string,
} = PropTypes;

const Visualization = React.createClass({
  propTypes: {
    autoRefresh: number.isRequired,
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    queryConfigs: arrayOf(shape({})).isRequired,
    name: string,
    activeQueryIndex: number,
    height: string,
    heightPixels: number,
  },

  contextTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      isGraphInView: true,
    };
  },

  handleToggleView() {
    this.setState({isGraphInView: !this.state.isGraphInView});
  },

  render() {
    const {queryConfigs, autoRefresh, timeRange, activeQueryIndex, height, heightPixels} = this.props;
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
    const isInDataExplorer = true;

    return (
      <div className={classNames("graph", {active: true})} style={{height}}>
        <div className="graph-heading">
          <div className="graph-title">
            {name || "Graph"}
          </div>
          <div className="graph-actions">
            <ul className="toggle toggle-sm">
              <li onClick={this.handleToggleView} className={classNames("toggle-btn ", {active: isGraphInView})}>Graph</li>
              <li onClick={this.handleToggleView} className={classNames("toggle-btn ", {active: !isGraphInView})}>Table</li>
            </ul>
          </div>
        </div>
        <div className={classNames({"graph-container": isGraphInView, "table-container": !isGraphInView})}>
          {isGraphInView ? (
            <RefreshingLineGraph
              queries={queries}
              autoRefresh={autoRefresh}
              activeQueryIndex={activeQueryIndex}
              isInDataExplorer={isInDataExplorer}
              />
          ) : <MultiTable queries={queries} height={heightPixels} />}
        </div>
      </div>
    );
  },
});

export default Visualization;
