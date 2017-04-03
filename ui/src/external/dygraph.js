import Dygraph from 'dygraphs/dygraph-combined'

export default Dygraph

/* eslint-disable */
Dygraph.prototype.findClosestPoint = function(domX, domY) {
  if (Dygraph.VERSION !== '1.1.1') {
    console.error('Dygraph version changed from expected - re-copy findClosestPoint');
  }
  var minXDist = Infinity;
  var minYDist = Infinity;
  var xdist, ydist, dx, dy, point, closestPoint, closestSeries, closestRow;
  for ( var setIdx = this.layout_.points.length - 1 ; setIdx >= 0 ; --setIdx ) {
    var points = this.layout_.points[setIdx];
    for (var i = 0; i < points.length; ++i) {
      point = points[i];
      if (!Dygraph.isValidPoint(point)) continue;

      dx = point.canvasx - domX;
      dy = point.canvasy - domY;

      xdist = dx * dx;
      ydist = dy * dy;

      if (xdist < minXDist) {
        minXDist = xdist;
        minYDist = ydist;
        closestRow = point.idx;
        closestSeries = setIdx;
      } else if (xdist === minXDist && ydist < minYDist) {
        minXDist = xdist;
        minYDist = ydist;
        closestRow = point.idx;
        closestSeries = setIdx;
      }
    }
  }
  var name = this.layout_.setNames[closestSeries];
  return {
    row: closestRow,
    seriesName: name,
    point: closestPoint
  };
};
/* eslint-enable */

