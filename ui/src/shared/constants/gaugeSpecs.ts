import {InfluxColors} from '@influxdata/clockface'

export interface GaugeTheme {
  degree: number
  lineCount: number
  smallLineCount: number
  lineColor: string
  labelColor: string
  labelFontSize: number
  lineStrokeSmall: number
  lineStrokeLarge: number
  tickSizeSmall: number
  tickSizeLarge: number
  minFontSize: number
  minLineWidth: number
  valueColor: string
  needleColor0: string
  needleColor1: string
  overflowDelta: number
}

export const GAUGE_THEME_LIGHT: GaugeTheme = {
  degree: (5 / 4) * Math.PI,
  lineCount: 5,
  smallLineCount: 10,
  lineColor: `${InfluxColors.Platinum}`,
  labelColor: `${InfluxColors.Storm}`,
  labelFontSize: 13,
  lineStrokeSmall: 1,
  lineStrokeLarge: 3,
  tickSizeSmall: 9,
  tickSizeLarge: 18,
  minFontSize: 22,
  minLineWidth: 24,
  valueColor: `${InfluxColors.Graphite}`,
  needleColor0: `${InfluxColors.Wolf}`,
  needleColor1: `${InfluxColors.Smoke}`,

  // This constant expresses how far past the gauge max the needle should be
  // drawn if the value for the needle is greater than the gauge max. It is
  // expressed as a percentage of the circumference of a circle, e.g. 0.5 means
  // draw halfway around the gauge from the max value
  overflowDelta: 0.03,
}

export const GAUGE_THEME_DARK: GaugeTheme = {
  degree: (5 / 4) * Math.PI,
  lineCount: 5,
  smallLineCount: 10,
  lineColor: `${InfluxColors.Graphite}`,
  labelColor: `${InfluxColors.Wolf}`,
  labelFontSize: 13,
  lineStrokeSmall: 1,
  lineStrokeLarge: 3,
  tickSizeSmall: 9,
  tickSizeLarge: 18,
  minFontSize: 22,
  minLineWidth: 24,
  valueColor: `${InfluxColors.White}`,
  needleColor0: `${InfluxColors.Smoke}`,
  needleColor1: `${InfluxColors.White}`,

  // This constant expresses how far past the gauge max the needle should be
  // drawn if the value for the needle is greater than the gauge max. It is
  // expressed as a percentage of the circumference of a circle, e.g. 0.5 means
  // draw halfway around the gauge from the max value
  overflowDelta: 0.03,
}
