export const drawCircle = (
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  diameter: number = 5
) => {
  ctx.lineWidth = diameter
  ctx.beginPath()
  ctx.lineCap = 'round'
  ctx.moveTo(x, y)
  ctx.lineTo(x, y)
  ctx.stroke()
  ctx.lineCap = 'butt'
}

export const drawSquare = (
  ctx: CanvasRenderingContext2D,
  centerX: number,
  centerY: number,
  size: number = 4
) => {
  const x = centerX - size / 2
  const y = centerY - size / 2

  ctx.rect(x, y, size, size)
  ctx.fill()
}

export const drawTriangle = (
  ctx: CanvasRenderingContext2D,
  centerX: number,
  centerY: number,
  size: number = 3
) => {
  ctx.beginPath()
  ctx.moveTo(centerX - size, centerY + size)
  ctx.lineTo(centerX + size, centerY + size)
  ctx.lineTo(centerX, centerY - size)
  ctx.fill()
}

export const drawPlus = (
  ctx: CanvasRenderingContext2D,
  centerX: number,
  centerY: number,
  size: number = 6
) => {
  const mid = size / 2

  ctx.lineWidth = 1

  ctx.beginPath()
  ctx.moveTo(centerX - mid, centerY)
  ctx.lineTo(centerX + mid, centerY)
  ctx.stroke()

  ctx.beginPath()
  ctx.moveTo(centerX, centerY - mid)
  ctx.lineTo(centerX, centerY + mid)
  ctx.stroke()
}

export const drawEx = (
  ctx: CanvasRenderingContext2D,
  centerX: number,
  centerY: number,
  size: number = 6
) => {
  const mid = size / 2

  ctx.lineWidth = 1

  ctx.beginPath()
  ctx.moveTo(centerX - mid, centerY - mid)
  ctx.lineTo(centerX + mid, centerY + mid)
  ctx.closePath()
  ctx.stroke()

  ctx.beginPath()
  ctx.moveTo(centerX - mid, centerY + mid)
  ctx.lineTo(centerX + mid, centerY - mid)
  ctx.closePath()
  ctx.stroke()
}

export const drawTritip = (
  ctx: CanvasRenderingContext2D,
  centerX: number,
  centerY: number,
  size: number = 3
) => {
  const cos30 = 0.86602540378
  const sin30 = 0.5

  ctx.lineWidth = 1

  ctx.beginPath()
  ctx.moveTo(centerX, centerY)
  ctx.lineTo(centerX + cos30 * size, centerY + sin30 * size)
  ctx.closePath()
  ctx.stroke()

  ctx.beginPath()
  ctx.moveTo(centerX, centerY)
  ctx.lineTo(centerX - cos30 * size, centerY + sin30 * size)
  ctx.closePath()
  ctx.stroke()

  ctx.beginPath()
  ctx.moveTo(centerX, centerY)
  ctx.lineTo(centerX, centerY - size)
  ctx.closePath()
  ctx.stroke()
}
