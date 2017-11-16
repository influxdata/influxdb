import React, {Component, PropTypes} from 'react'

const colors = ['#BF3D5E', '#F95F53', '#FFD255', '#7CE490']

class Gauge extends Component {
  constructor(props) {
    super(props)

    this.state = {
      gaugePosition: this.props.gaugePosition,
    }
  }

  componentDidMount() {
    this.updateCanvas()
  }

  updateCanvas = () => {
    const canvas = this.canvasRef
    const ctx = canvas.getContext('2d')

    const centerX = canvas.width / 2
    const centerY = canvas.height / 2 * 1.06
    const radius = canvas.width / 2 * 0.5

    const gradientThickness = 20

    this.drawMultiRadiantCircle(
      ctx,
      centerX,
      centerY,
      radius,
      colors,
      gradientThickness
    )
    this.drawGaugeLines(ctx, centerX, centerY, radius, gradientThickness)
    // this.drawGaugeLabels(ctx, centerX, centerY, radius, gradientThickness)
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
    const gaugeLineColor = '#545667'
    const arcStart = Math.PI * 0.75
    const arcLength = Math.PI * 1.5
    const arcStop = arcStart + arcLength
    const numSteps = 6
    const numSmallSteps = 30
    const degree = Math.PI / 180
    const startDegree = degree * 135
    const arcLargeIncrement = arcLength / numSteps
    const arcSmallIncrement = arcLength / numSmallSteps

    // Semi-circle
    const arcRadius = radius + gradientThickness * 0.8
    ctx.beginPath()
    ctx.arc(xc, yc, arcRadius, arcStart, arcStop)
    ctx.lineWidth = 3
    ctx.lineCap = 'round'
    ctx.strokeStyle = gaugeLineColor
    ctx.stroke()
    ctx.closePath()

    // Match center of canvas to center of gauge
    ctx.translate(xc, yc)

    // Tick lines
    const largeTickSize = 18
    const smallTickSize = 9

    // Draw Large ticks
    for (let lt = 0; lt <= numSteps; lt++) {
      // Rototion before drawing line
      ctx.rotate(startDegree)
      ctx.rotate(lt * arcLargeIncrement)
      // Draw line
      ctx.beginPath()
      ctx.lineWidth = 3
      ctx.lineCap = 'round'
      ctx.strokeStyle = gaugeLineColor
      ctx.moveTo(arcRadius, 0)
      ctx.lineTo(arcRadius + largeTickSize, 0)
      ctx.stroke()
      ctx.closePath()
      // Return to starting rotation
      ctx.rotate(-lt * arcLargeIncrement)
      ctx.rotate(-startDegree)
    }

    // Draw Small ticks
    for (let lt = 0; lt <= numSmallSteps; lt++) {
      // Rototion before drawing line
      ctx.rotate(startDegree)
      ctx.rotate(lt * arcSmallIncrement)
      // Draw line
      ctx.beginPath()
      ctx.lineWidth = 1
      ctx.lineCap = 'round'
      ctx.strokeStyle = gaugeLineColor
      ctx.moveTo(arcRadius, 0)
      ctx.lineTo(arcRadius + smallTickSize, 0)
      ctx.stroke()
      ctx.closePath()
      // Return to starting rotation
      ctx.rotate(-lt * arcSmallIncrement)
      ctx.rotate(-startDegree)
    }
  }

  drawGaugeLabels = (ctx, xc, yc, radius, gradientThickness) => {
    const {minValue, maxValue} = this.props
    const rad = radius + gradientThickness
    const numSteps = 6
    const rangeStep = (maxValue - minValue) / numSteps

    const gaugeValues = []
    for (let g = minValue; g <= maxValue; g += rangeStep) {
      gaugeValues.push(g.toString())
    }

    const degree = Math.PI / 180

    const startDegree = degree * (135 + 180)
    const arcLength = Math.PI * 1.5
    const arcIncrement = arcLength / numSteps

    ctx.font = '20px Roboto'
    ctx.fillStyle = 'white'
    ctx.textAlign = 'right'
    ctx.translate(xc, yc)
    ctx.rotate(startDegree)
    // ctx.fillText(gaugeValues[0], -rad, 0)
    // ctx.fillRect(0, 0, -rad, 2)

    for (let i = 0; i <= numSteps; i++) {
      ctx.fillText(gaugeValues[i], -rad, 7)
      // ctx.fillRect(0, -1, -rad, 2)
      ctx.rotate(arcIncrement)
    }
    //   ctx.font = '20px Roboto'
    //   ctx.fillStyle = 'white'
    //   ctx.textAlign = 'right'
    //   ctx.translate(xc, yc)
    //   ctx.rotate(startDegree + i * arcIncrement)
    //   ctx.fillText(gaugeValues[1], -rad, 0)
    //   ctx.fillRect(0, 0, -rad, 2)
    //   ctx.translate(xc, yc)
    // }
  }

  render() {
    return (
      <canvas
        className="gauge"
        style={{border: '1px solid #f00'}}
        width={400}
        height={300}
        ref={r => (this.canvasRef = r)}
      />
    )
  }
}

const {number} = PropTypes

Gauge.propTypes = {
  minValue: number.isRequired,
  maxValue: number.isRequired,
  gaugePosition: number.isRequired,
}

export default Gauge
