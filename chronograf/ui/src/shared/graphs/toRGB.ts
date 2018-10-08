// Helper functions copied from dygraphs/src/dygraph-utils

const RGBA_RE = /^rgba?\((\d{1,3}),\s*(\d{1,3}),\s*(\d{1,3})(?:,\s*([01](?:\.\d+)?))?\)$/

function parseRGBA(rgbStr) {
  const bits = RGBA_RE.exec(rgbStr)

  if (!bits) {
    return null
  }

  const r = parseInt(bits[1], 10)
  const g = parseInt(bits[2], 10)
  const b = parseInt(bits[3], 10)

  if (bits[4]) {
    return {r, g, b, a: parseFloat(bits[4])}
  } else {
    return {r, g, b}
  }
}

export function toRGB(colorStr) {
  // Strategy: First try to parse colorStr directly. This is fast & avoids DOM
  // manipulation.  If that fails (e.g. for named colors like 'red'), then
  // create a hidden DOM element and parse its computed color.
  const rgb = parseRGBA(colorStr)

  if (rgb) {
    return rgb
  }

  const div = document.createElement('div')

  div.style.backgroundColor = colorStr
  div.style.visibility = 'hidden'
  document.body.appendChild(div)

  const rgbStr = window.getComputedStyle(div, null).backgroundColor

  document.body.removeChild(div)

  return parseRGBA(rgbStr)
}
