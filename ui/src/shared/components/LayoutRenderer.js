import React, {PropTypes} from 'react';
import AutoRefresh from 'shared/components/AutoRefresh';
import LineGraph from 'shared/components/LineGraph';
import SingleStat from 'shared/components/SingleStat';
import ReactGridLayout, {WidthProvider} from 'react-grid-layout';
const GridLayout = WidthProvider(ReactGridLayout);
import _ from 'lodash';

const RefreshingLineGraph = AutoRefresh(LineGraph);
const RefreshingSingleStat = AutoRefresh(SingleStat);

const {
  arrayOf,
  number,
  shape,
  string,
} = PropTypes;

export const LayoutRenderer = React.createClass({
  propTypes: {
    timeRange: shape({
      defaultGroupBy: string.isRequired,
      queryValue: string.isRequired,
    }).isRequired,
    cells: arrayOf(
      shape({
        queries: arrayOf(
          shape({
            label: string,
            range: shape({
              upper: number,
              lower: number,
            }),
            rp: string,
            text: string.isRequired,
            database: string.isRequired,
            groupbys: arrayOf(string),
            wheres: arrayOf(string),
          }).isRequired
        ).isRequired,
        x: number.isRequired,
        y: number.isRequired,
        w: number.isRequired,
        h: number.isRequired,
        i: string.isRequired,
        name: string.isRequired,
      }).isRequired
    ),
    autoRefreshMs: number.isRequired,
    host: string,
    source: string,
  },

  getInitialState() {
    return ({
      layout: _.without(this.props.cells, ['queries']),
    });
  },

  buildQuery(q) {
    const {timeRange, host} = this.props;
    const {wheres, groupbys} = q;

    let text = q.text;

    text += ` where time > ${timeRange.queryValue}`;

    if (host) {
      text += ` and \"host\" = '${host}'`;
    }

    if (wheres && wheres.length > 0) {
      text += ` and ${wheres.join(' and ')}`;
    }

    if (groupbys) {
      if (groupbys.find((g) => g.includes("time"))) {
        text += ` group by ${groupbys.join(',')}`;
      } else if (groupbys.length > 0) {
        text += ` group by time(${timeRange.defaultGroupBy}),${groupbys.join(',')}`;
      } else {
        text += ` group by time(${timeRange.defaultGroupBy})`;
      }
    } else {
      text += ` group by time(${timeRange.defaultGroupBy})`;
    }

    return text;
  },

  generateVisualizations() {
    const {autoRefreshMs, source, cells} = this.props;

    return cells.map((cell) => {
      const qs = cell.queries.map((q) => {
        return Object.assign({}, q, {
          host: source,
          text: this.buildQuery(q),
        });
      });


      if (cell.type === 'single-stat') {
        return (
          <div key={cell.i}>
            <h2 className="hosts-graph-heading">{cell.name || `Graph`}</h2>
            <div className="hosts-graph graph-container">
              <RefreshingSingleStat queries={[qs[0]]} autoRefresh={autoRefreshMs} />
            </div>
          </div>
        );
      }

      return (
        <div key={cell.i}>
          <h2 className="hosts-graph-heading">{cell.name || `Graph`}</h2>
          <div className="hosts-graph graph-container">
            <RefreshingLineGraph queries={qs} autoRefresh={autoRefreshMs} showSingleStat={cell.type === "line-plus-single-stat"} />
          </div>
        </div>
      );
    });
  },

  render() {
    const layoutMargin = 4;
    return (
      <GridLayout
        layout={this.state.layout}
        cols={12}
        rowHeight={83.5}
        margin={[layoutMargin, layoutMargin]}
        containerPadding={[0, 0]}
        useCSSTransforms={false}
        onResize={triggerWindowResize}
        onLayoutChange={triggerWindowResize}
      >
        {this.generateVisualizations()}
      </GridLayout>
    );

    function triggerWindowResize() {
      // Hack to get dygraphs to fit properly during and after resize (dispatchEvent is a global method on window).
      const evt = document.createEvent('CustomEvent');  // MUST be 'CustomEvent'
      evt.initCustomEvent('resize', false, false, null);
      dispatchEvent(evt);
    }
  },
});

export default LayoutRenderer;