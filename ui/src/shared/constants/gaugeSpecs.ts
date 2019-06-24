export const GAUGE_SPECS = {
  degree: (5 / 4) * Math.PI,
  lineCount: 5,
  smallLineCount: 10,
  lineColor: '#545667',
  labelColor: '#8E91A1',
  labelFontSize: 13,
  lineStrokeSmall: 1,
  lineStrokeLarge: 3,
  tickSizeSmall: 9,
  tickSizeLarge: 18,
  minFontSize: 22,
  minLineWidth: 24,
  valueColor: '#ffffff',
  needleColor0: '#434453',
  needleColor1: '#ffffff',

  // This constant expresses how far past the gauge max the needle should be
  // drawn if the value for the needle is greater than the gauge max. It is
  // expressed as a percentage of the circumference of a circle, e.g. 0.5 means
  // draw halfway around the gauge from the max value
  overflowDelta: 0.03,
}
