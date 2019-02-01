export const clearCanvas = (
  canvas: HTMLCanvasElement,
  width: number,
  height: number
) => {
  const context = canvas.getContext('2d')
  const dpRatio = window.devicePixelRatio || 1

  // Configure canvas to draw on retina displays correctly
  canvas.width = width * dpRatio
  canvas.height = height * dpRatio
  canvas.style.width = `${width}px`
  canvas.style.height = `${height}px`
  context.scale(dpRatio, dpRatio)

  context.clearRect(0, 0, width, height)
}
