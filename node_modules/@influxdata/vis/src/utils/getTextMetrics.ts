export interface TextMetrics {
  width: number
  height: number
}

export const getTextMetrics = (font: string, text: string): TextMetrics => {
  const div = document.createElement('div')
  const span = document.createElement('span')

  div.appendChild(span)

  // Translate offscreen
  div.setAttribute(
    'style',
    `font: ${font}; display: inline; position: fixed; transform: translate(-9999px, -9999px);`
  )

  document.body.appendChild(div)

  span.innerText = text

  const metrics = {
    width: span.offsetWidth,
    height: span.offsetHeight,
  }

  document.body.removeChild(div)

  return metrics
}
