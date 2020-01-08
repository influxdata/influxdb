// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Utils
import {formatStatValue} from 'src/shared/utils/formatStatValue'

// Constants
import {GAUGE_SPECS} from 'src/shared/constants/gaugeSpecs'
import {
  COLOR_TYPE_MIN,
  COLOR_TYPE_MAX,
  DEFAULT_VALUE_MAX,
  DEFAULT_VALUE_MIN,
  MIN_THRESHOLDS,
} from 'src/shared/constants/thresholds'

// Types
import {Color} from 'src/types/colors'
import {DecimalPlaces} from 'src/types/dashboards'

interface Props {
  width: number
  height: number
  gaugePosition: number
  colors?: Color[]
  prefix: string
  tickPrefix: string
  suffix: string
  tickSuffix: string
  decimalPlaces: DecimalPlaces
}

@ErrorHandling
class Gauge extends Component<Props> {
  private canvasRef: React.RefObject<HTMLCanvasElement>

  constructor(props: Props) {
    super(props)
    this.canvasRef = React.createRef()
  }

  public componentDidMount() {
    this.updateCanvas()
  }

  public componentDidUpdate() {
    this.updateCanvas()
  }

  public render() {
    const {width, height} = this.props

    return (
      <canvas
        className="gauge"
        width={width}
        height={height}
        ref={this.canvasRef}
      />
    )
  }

  private updateCanvas = () => {
    this.resetCanvas()

    const canvas = this.canvasRef.current
    const ctx = canvas.getContext('2d')
    const {width, height} = this.props

    const centerX = width / 2
    const centerY = (height / 2) * 1.13
    const radius = (Math.min(width, height) / 2) * 0.5

    const {minLineWidth, minFontSize} = GAUGE_SPECS
    const gradientThickness = Math.max(minLineWidth, radius / 4)
    const labelValueFontSize = Math.max(minFontSize, radius / 4)

    const {colors} = this.props
    if (!colors || colors.length === 0) {
      return
    }

    // Distill out max and min values
    const minValue = Number(
      _.get(
        colors.find(color => color.type === COLOR_TYPE_MIN),
        'value',
        DEFAULT_VALUE_MIN
      )
    )
    const maxValue = Number(
      _.get(
        colors.find(color => color.type === COLOR_TYPE_MAX),
        'value',
        DEFAULT_VALUE_MAX
      )
    )

    // The following functions must be called in the specified order
    if (colors.length === MIN_THRESHOLDS) {
      this.drawGradientGauge(ctx, centerX, centerY, radius, gradientThickness)
    } else {
      this.drawSegmentedGauge(
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
    this.drawGaugeLabels(ctx, radius, gradientThickness, minValue, maxValue)
    this.drawGaugeValue(ctx, radius, labelValueFontSize)
    this.drawNeedle(ctx, radius, minValue, maxValue)
  }

  private resetCanvas = () => {
    const canvas = this.canvasRef.current
    const ctx = canvas.getContext('2d')
    const {width, height} = this.props
    const dpRatio = window.devicePixelRatio || 1

    // Set up canvas to draw on HiDPI / Retina screens correctly
    canvas.width = width * dpRatio
    canvas.height = height * dpRatio
    canvas.style.width = `${width}px`
    canvas.style.height = `${height}px`
    ctx.scale(dpRatio, dpRatio)

    // Clear the canvas
    ctx.clearRect(0, 0, width, height)
  }

  private drawGradientGauge = (ctx, xc, yc, r, gradientThickness) => {
    const {colors} = this.props
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

  private drawSegmentedGauge = (
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

  private drawGaugeLines = (ctx, xc, yc, radius, gradientThickness) => {
    const {
      degree,
      lineCount,
      lineColor,
      lineStrokeSmall,
      lineStrokeLarge,
      tickSizeSmall,
      tickSizeLarge,
      smallLineCount,
    } = GAUGE_SPECS

    const arcStart = Math.PI * 0.75
    const arcLength = Math.PI * 1.5
    const arcStop = arcStart + arcLength
    const totalSmallLineCount = lineCount * smallLineCount

    const startDegree = degree * 135
    const arcLargeIncrement = arcLength / lineCount
    const arcSmallIncrement = arcLength / totalSmallLineCount

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
      // Rotation before drawing line
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
    for (let lt = 0; lt <= totalSmallLineCount; lt++) {
      // Rotation before drawing line
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

  private drawGaugeLabels = (
    ctx,
    radius,
    gradientThickness,
    minValue,
    maxValue
  ) => {
    const {prefix, suffix, decimalPlaces} = this.props
    const {degree, lineCount, labelColor, labelFontSize} = GAUGE_SPECS

    const tickValues = [
      ..._.range(minValue, maxValue, Math.abs(maxValue - minValue) / lineCount),
      maxValue,
    ]

    const labels = tickValues.map(tick =>
      formatStatValue(tick, {decimalPlaces, prefix, suffix})
    )

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
      ctx.fillText(labels[i], 0, 0)
      ctx.rotate(startDegree)
      ctx.rotate(i * arcIncrement)
      ctx.translate(-labelRadius, 0)
      ctx.rotate(i * -arcIncrement)
      ctx.rotate(-startDegree)
    }
  }

  private drawGaugeValue = (ctx, radius, labelValueFontSize) => {
    const {gaugePosition, prefix, suffix, decimalPlaces} = this.props
    const {valueColor} = GAUGE_SPECS

    ctx.font = `${labelValueFontSize}px Roboto`
    ctx.fillStyle = valueColor
    ctx.textBaseline = 'middle'
    ctx.textAlign = 'center'

    const textY = radius
    const textContent = formatStatValue(gaugePosition, {
      decimalPlaces,
      prefix,
      suffix
    })

    ctx.fillText(textContent, 0, textY)
  }

  private drawNeedle = (ctx, radius, minValue, maxValue) => {
    const {gaugePosition} = this.props
    const {degree, needleColor0, needleColor1, overflowDelta} = GAUGE_SPECS
    const arcDistance = Math.PI * 1.5

    let needleRotation: number

    if (gaugePosition <= minValue) {
      needleRotation = 0 - overflowDelta
    } else if (gaugePosition >= maxValue) {
      needleRotation = 1 + overflowDelta
    } else {
      needleRotation = (gaugePosition - minValue) / (maxValue - minValue)
    }

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
}

export default Gauge
