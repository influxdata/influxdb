import React, {Component, PropTypes} from 'react'

import {GAUGE_SPECS} from 'shared/constants/gaugeSpecs'

class Gauge extends Component {
  constructor(props) {
    super(props)

    this.state = {
      gaugePosition: this.props.gaugePosition,
    }
  }

  componentDidMount() {
    this.validateValues()
    this.updateCanvas()
  }

  componentDidUpdate() {
    this.validateValues()
    this.updateCanvas()
  }

  validateValues = () => {
    const {
      minValue,
      maxValue,
      lowerThreshold,
      upperThreshold,
      gaugePosition,
    } = this.props

    if (
      !(minValue < lowerThreshold < upperThreshold < maxValue) ||
      gaugePosition < minValue ||
      gaugePosition > maxValue
    ) {
      console.error('Gauge component has received bad values')
    }
  }
  resetCanvas = (canvas, context) => {
    context.setTransform(1, 0, 0, 1, 0, 0)
    context.clearRect(0, 0, canvas.width, canvas.height)
  }

  updateCanvas = () => {
    const canvas = this.canvasRef
    const ctx = canvas.getContext('2d')

    this.resetCanvas(canvas, ctx)

    const centerX = canvas.width / 2
    const centerY = canvas.height / 2 * 1.13
    const radius = canvas.width / 2 * 0.5

    const gradientThickness = 20

    // The following functions must be called in the specified order
    this.drawGauge(ctx, centerX, centerY, radius)
    this.drawGaugeLines(ctx, centerX, centerY, radius, gradientThickness)
    this.drawGaugeLabels(ctx, centerX, centerY, radius, gradientThickness)
    this.drawGaugeValue(ctx, radius)
    this.drawNeedle(ctx, radius)
  }

  drawGauge = (ctx, xc, yc, r) => {
    const {lowerThreshold, upperThreshold, minValue, maxValue} = this.props

    const trueValueRange = Math.abs(maxValue - minValue)
    const totalArcLength = Math.PI * 1.5
    const arcOffset = Math.PI * 0.75

    // Draw lower section
    const lowerArcStart = Math.PI * 0.75
    const lowerArcEnd =
      (lowerThreshold - minValue) / trueValueRange * totalArcLength + arcOffset

    // Determine coordinates for gradient
    const xLowerStart = xc + Math.cos(lowerArcStart) * r
    const yLowerStart = yc + Math.sin(lowerArcStart) * r
    const xLowerEnd = xc + Math.cos(lowerArcEnd) * r
    const yLowerEnd = yc + Math.sin(lowerArcEnd) * r

    // Define lower section gradient
    const lowerGradient = ctx.createLinearGradient(
      xLowerStart,
      yLowerStart,
      xLowerEnd,
      yLowerEnd
    )
    lowerGradient.addColorStop(0, 'green')
    lowerGradient.addColorStop(1.0, 'yellow')

    ctx.beginPath()
    ctx.lineWidth = 20
    ctx.strokeStyle = lowerGradient
    ctx.arc(xc, yc, r, lowerArcStart, lowerArcEnd)
    ctx.stroke()

    // Draw upper section
    const upperArcStart =
      (upperThreshold - minValue) / trueValueRange * totalArcLength + arcOffset
    const upperArcEnd = Math.PI * 0.25

    // Determine coordinates for gradient
    const xUpperStart = xc + Math.cos(upperArcStart) * r
    const yUpperStart = yc + Math.sin(upperArcStart) * r
    const xUpperEnd = xc + Math.cos(upperArcEnd) * r
    const yUpperEnd = yc + Math.sin(upperArcEnd) * r

    // Define lower section gradient
    const upperGradient = ctx.createLinearGradient(
      xUpperStart,
      yUpperStart,
      xUpperEnd,
      yUpperEnd
    )
    upperGradient.addColorStop(0, 'orange')
    upperGradient.addColorStop(1.0, 'red')

    ctx.beginPath()
    ctx.lineWidth = 20
    ctx.strokeStyle = upperGradient
    ctx.arc(xc, yc, r, upperArcStart, upperArcEnd)
    ctx.stroke()

    // Draw middle section
    // Determine coordinates for gradient
    const xMiddleStart = xc + Math.cos(lowerArcEnd) * r
    const yMiddleStart = yc + Math.sin(lowerArcEnd) * r
    const xMiddleEnd = xc + Math.cos(upperArcStart) * r
    const yMiddleEnd = yc + Math.sin(upperArcStart) * r
    // Define lower section gradient
    const middleGradient = ctx.createLinearGradient(
      xMiddleStart,
      yMiddleStart,
      xMiddleEnd,
      yMiddleEnd
    )
    middleGradient.addColorStop(0, 'yellow')
    middleGradient.addColorStop(1.0, 'orange')

    ctx.beginPath()
    ctx.lineWidth = 20
    ctx.lineCap = 'butt'
    ctx.strokeStyle = middleGradient
    ctx.arc(xc, yc, r, lowerArcEnd, upperArcStart)
    ctx.stroke()
  }

  drawMultiRadiantCircle = (
    ctx,
    xc,
    yc,
    r,
    radientColors,
    gradientThickness
  ) => {
    const partLength = Math.PI / (radientColors.length - 2)
    let start = Math.PI * 0.75
    let gradient = null
    let startColor = null
    let endColor = null

    for (let i = 0; i < radientColors.length - 1; i++) {
      startColor = radientColors[i]
      endColor = radientColors[(i + 1) % radientColors.length]

      // x start / end of the next arc to draw
      const xStart = xc + Math.cos(start) * r
      const xEnd = xc + Math.cos(start + partLength) * r
      // y start / end of the next arc to draw
      const yStart = yc + Math.sin(start) * r
      const yEnd = yc + Math.sin(start + partLength) * r

      ctx.beginPath()

      gradient = ctx.createLinearGradient(xStart, yStart, xEnd, yEnd)
      gradient.addColorStop(0, startColor)
      gradient.addColorStop(1.0, endColor)

      ctx.strokeStyle = gradient
      ctx.arc(xc, yc, r, start, start + partLength)
      ctx.lineWidth = gradientThickness
      ctx.stroke()
      ctx.closePath()

      start += partLength
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

  drawGaugeLabels = (ctx, xc, yc, radius, gradientThickness) => {
    const {degree, lineCount} = GAUGE_SPECS

    // Build array of label strings
    const {minValue, maxValue} = this.props
    const incrementValue = (maxValue - minValue) / lineCount

    const gaugeValues = []
    for (let g = minValue; g <= maxValue; g += incrementValue) {
      gaugeValues.push(g.toString())
    }

    const startDegree = degree * 135
    const arcLength = Math.PI * 1.5
    const arcIncrement = arcLength / lineCount

    // Format labels text
    ctx.font = 'bold 13px Helvetica'
    ctx.fillStyle = '#8E91A1'
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

  drawGaugeValue = (ctx, radius) => {
    const {gaugePosition} = this.props

    ctx.font = '40px Roboto'
    ctx.fillStyle = '#ffffff'
    ctx.textBaseline = 'middle'
    ctx.textAlign = 'center'

    const textY = radius * 0.75
    ctx.fillText(gaugePosition.toString(), 0, textY)
  }

  drawNeedle = (ctx, radius) => {
    const {minValue, maxValue, gaugePosition} = this.props
    const {degree} = GAUGE_SPECS
    const arcDistance = Math.PI * 1.5

    const needleRotation = (gaugePosition - minValue) / (maxValue - minValue)

    const needleGradient = ctx.createLinearGradient(0, -10, 0, radius)
    needleGradient.addColorStop(0, '#434453')
    needleGradient.addColorStop(1, 'white')

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

const {number, string} = PropTypes

Gauge.propTypes = {
  width: string.isRequired,
  height: string.isRequired,
  minValue: number.isRequired,
  maxValue: number.isRequired,
  lowerThreshold: number.isRequired,
  upperThreshold: number.isRequired,
  gaugePosition: number.isRequired,
}

export default Gauge
