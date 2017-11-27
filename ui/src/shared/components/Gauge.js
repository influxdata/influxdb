import React, {Component, PropTypes} from 'react'
import _ from 'lodash'

import {GAUGE_SPECS} from 'shared/constants/gaugeSpecs'

import {
  COLOR_TYPE_MIN,
  COLOR_TYPE_MAX,
  MIN_THRESHOLDS,
  DEFAULT_COLORS,
} from 'src/dashboards/constants/gaugeColors'

class Gauge extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    this.updateCanvas()
  }

  componentDidUpdate() {
    this.updateCanvas()
  }

  resetCanvas = (canvas, context) => {
    context.setTransform(1, 0, 0, 1, 0, 0)
    context.clearRect(0, 0, canvas.width, canvas.height)
  }

  updateCanvas = () => {
    const canvas = this.canvasRef
    canvas.width = canvas.height * (canvas.clientWidth / canvas.clientHeight)
    const ctx = canvas.getContext('2d')

    this.resetCanvas(canvas, ctx)

    const centerX = canvas.width / 2
    const centerY = canvas.height / 2 * 1.13
    const radius = Math.min(canvas.width, canvas.height) / 2 * 0.5

    const {minLineWidth, minFontSize} = GAUGE_SPECS
    const gradientThickness = Math.max(minLineWidth, radius / 4)
    const labelValueFontSize = Math.max(minFontSize, radius / 4)

    // Distill out max and min values
    let gaugeColors = this.props.colors
    if (gaugeColors.length === 0) {
      gaugeColors = DEFAULT_COLORS
    }
    const minValue = Number(
      gaugeColors.find(color => color.type === COLOR_TYPE_MIN).value
    )
    const maxValue = Number(
      gaugeColors.find(color => color.type === COLOR_TYPE_MAX).value
    )

    // The following functions must be called in the specified order
    if (gaugeColors.length === MIN_THRESHOLDS) {
      this.drawGradientGauge(
        gaugeColors,
        ctx,
        centerX,
        centerY,
        radius,
        gradientThickness
      )
    } else {
      this.drawSegmentedGauge(
        gaugeColors,
        ctx,
        centerX,
        centerY,
        radius,
        minValue,
        maxValue,
        gradientThickness
      )
    }
    this.drawGaugeLines(ctx, centerX, centerY, radius, gradientThickness)
    this.drawGaugeLabels(
      ctx,
      centerX,
      centerY,
      radius,
      gradientThickness,
      minValue,
      maxValue
    )
    this.drawGaugeValue(ctx, radius, labelValueFontSize)
    this.drawNeedle(ctx, radius, minValue, maxValue)
  }

  drawGradientGauge = (colors, ctx, xc, yc, r, gradientThickness) => {
    const sortedColors = _.sortBy(colors, color => Number(color.value))

    const arcStart = Math.PI * 0.75
    const arcEnd = arcStart + Math.PI * 1.5

    // Determine coordinates for gradient
    const xStart = xc + Math.cos(arcStart) * r
    const yStart = yc + Math.sin(arcStart) * r
    const xEnd = xc + Math.cos(arcEnd) * r
    const yEnd = yc + Math.sin(arcEnd) * r

    const gradient = ctx.createLinearGradient(xStart, yStart, xEnd, yEnd)
    gradient.addColorStop(0, sortedColors[0].hex)
    gradient.addColorStop(1.0, sortedColors[1].hex)

    ctx.beginPath()
    ctx.lineWidth = gradientThickness
    ctx.strokeStyle = gradient
    ctx.arc(xc, yc, r, arcStart, arcEnd)
    ctx.stroke()
  }

  drawSegmentedGauge = (
    ctx,
    xc,
    yc,
    r,
    minValue,
    maxValue,
    gradientThickness
  ) => {
    const {colors} = this.props
    const sortedColors = _.sortBy(colors, color => Number(color.value))

    const trueValueRange = Math.abs(maxValue - minValue)
    const totalArcLength = Math.PI * 1.5
    let startingPoint = Math.PI * 0.75

    // Iterate through colors, draw arc for each
    for (let c = 0; c < sortedColors.length - 1; c++) {
      // Use this color and the next to determine arc length
      const color = sortedColors[c]
      const nextColor = sortedColors[c + 1]

      // adjust values by subtracting minValue from them
      const adjustedValue = Number(color.value) - minValue
      const adjustedNextValue = Number(nextColor.value) - minValue

      const thisArc = Math.abs(adjustedValue - adjustedNextValue)
      // Multiply by arcLength to determine this arc's length
      const arcLength = totalArcLength * (thisArc / trueValueRange)
      // Draw arc
      ctx.beginPath()
      ctx.lineWidth = gradientThickness
      ctx.strokeStyle = color.hex
      ctx.arc(xc, yc, r, startingPoint, startingPoint + arcLength)
      ctx.stroke()
      // Add this arc's length to starting point
      startingPoint += arcLength
    }
  }

  drawGaugeLines = (ctx, xc, yc, radius, gradientThickness) => {
    const {
      degree,
      lineCount,
      lineColor,
      lineStrokeSmall,
      lineStrokeLarge,
      tickSizeSmall,
      tickSizeLarge,
    } = GAUGE_SPECS

    const arcStart = Math.PI * 0.75
    const arcLength = Math.PI * 1.5
    const arcStop = arcStart + arcLength
    const lineSmallCount = lineCount * 5
    const startDegree = degree * 135
    const arcLargeIncrement = arcLength / lineCount
    const arcSmallIncrement = arcLength / lineSmallCount

    // Semi-circle
    const arcRadius = radius + gradientThickness * 0.8
    ctx.beginPath()
    ctx.arc(xc, yc, arcRadius, arcStart, arcStop)
    ctx.lineWidth = 3
    ctx.lineCap = 'round'
    ctx.strokeStyle = lineColor
    ctx.stroke()
    ctx.closePath()

    // Match center of canvas to center of gauge
    ctx.translate(xc, yc)

    // Draw Large ticks
    for (let lt = 0; lt <= lineCount; lt++) {
      // Rototion before drawing line
      ctx.rotate(startDegree)
      ctx.rotate(lt * arcLargeIncrement)
      // Draw line
      ctx.beginPath()
      ctx.lineWidth = lineStrokeLarge
      ctx.lineCap = 'round'
      ctx.strokeStyle = lineColor
      ctx.moveTo(arcRadius, 0)
      ctx.lineTo(arcRadius + tickSizeLarge, 0)
      ctx.stroke()
      ctx.closePath()
      // Return to starting rotation
      ctx.rotate(-lt * arcLargeIncrement)
      ctx.rotate(-startDegree)
    }

    // Draw Small ticks
    for (let lt = 0; lt <= lineSmallCount; lt++) {
      // Rototion before drawing line
      ctx.rotate(startDegree)
      ctx.rotate(lt * arcSmallIncrement)
      // Draw line
      ctx.beginPath()
      ctx.lineWidth = lineStrokeSmall
      ctx.lineCap = 'round'
      ctx.strokeStyle = lineColor
      ctx.moveTo(arcRadius, 0)
      ctx.lineTo(arcRadius + tickSizeSmall, 0)
      ctx.stroke()
      ctx.closePath()
      // Return to starting rotation
      ctx.rotate(-lt * arcSmallIncrement)
      ctx.rotate(-startDegree)
    }
  }

  drawGaugeLabels = (
    ctx,
    xc,
    yc,
    radius,
    gradientThickness,
    minValue,
    maxValue
  ) => {
    const {degree, lineCount, labelColor, labelFontSize} = GAUGE_SPECS

    const incrementValue = (maxValue - minValue) / lineCount

    const gaugeValues = []
    for (let g = minValue; g < maxValue; g += incrementValue) {
      const roundedValue = Math.round(g * 100) / 100
      gaugeValues.push(roundedValue.toString())
    }
    gaugeValues.push((Math.round(maxValue * 100) / 100).toString())

    const startDegree = degree * 135
    const arcLength = Math.PI * 1.5
    const arcIncrement = arcLength / lineCount

    // Format labels text
    ctx.font = `bold ${labelFontSize}px Helvetica`
    ctx.fillStyle = labelColor
    ctx.textBaseline = 'middle'
    ctx.textAlign = 'right'
    let labelRadius

    for (let i = 0; i <= lineCount; i++) {
      if (i === 3) {
        ctx.textAlign = 'center'
        labelRadius = radius + gradientThickness + 30
      } else {
        labelRadius = radius + gradientThickness + 23
      }
      if (i > 3) {
        ctx.textAlign = 'left'
      }
      ctx.rotate(startDegree)
      ctx.rotate(i * arcIncrement)
      ctx.translate(labelRadius, 0)
      ctx.rotate(i * -arcIncrement)
      ctx.rotate(-startDegree)
      ctx.fillText(gaugeValues[i], 0, 0)
      ctx.rotate(startDegree)
      ctx.rotate(i * arcIncrement)
      ctx.translate(-labelRadius, 0)
      ctx.rotate(i * -arcIncrement)
      ctx.rotate(-startDegree)
    }
  }

  drawGaugeValue = (ctx, radius, labelValueFontSize) => {
    const {gaugePosition} = this.props
    const {valueColor} = GAUGE_SPECS

    ctx.font = `${labelValueFontSize}px Roboto`
    ctx.fillStyle = valueColor
    ctx.textBaseline = 'middle'
    ctx.textAlign = 'center'

    const textY = radius
    ctx.fillText(gaugePosition.toString(), 0, textY)
  }

  drawNeedle = (ctx, radius, minValue, maxValue) => {
    const {gaugePosition} = this.props
    const {degree, needleColor0, needleColor1} = GAUGE_SPECS
    const arcDistance = Math.PI * 1.5

    const needleRotation = (gaugePosition - minValue) / (maxValue - minValue)

    const needleGradient = ctx.createLinearGradient(0, -10, 0, radius)
    needleGradient.addColorStop(0, needleColor0)
    needleGradient.addColorStop(1, needleColor1)

    // Starting position of needle is at minimum
    ctx.rotate(degree * 45)
    ctx.rotate(arcDistance * needleRotation)
    ctx.beginPath()
    ctx.fillStyle = needleGradient
    ctx.arc(0, 0, 10, 0, Math.PI, true)
    ctx.lineTo(0, radius)
    ctx.lineTo(10, 0)
    ctx.fill()
  }

  render() {
    const {width, height} = this.props
    return (
      <canvas
        className="gauge"
        width={width}
        height={height}
        ref={r => (this.canvasRef = r)}
      />
    )
  }
}

const {arrayOf, number, shape, string} = PropTypes

Gauge.propTypes = {
  width: string.isRequired,
  height: string.isRequired,
  gaugePosition: number.isRequired,
  colors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: string.isRequired,
    }).isRequired
  ).isRequired,
}

export default Gauge
