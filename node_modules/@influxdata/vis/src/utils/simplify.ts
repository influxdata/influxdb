type ArrayLike = number[] | Float64Array

const sqDist = (x0: number, y0: number, x1: number, y1: number) => {
  return (y1 - y0) ** 2 + (x1 - x0) ** 2
}

const simplifyDist = (xs: ArrayLike, ys: ArrayLike, epsilon: number) => {
  const epsilonSq = epsilon ** 2
  const keep = new Uint8Array(xs.length)

  let prevX = xs[0]
  let prevY = ys[0]
  let keptLength = 1

  keep[0] = 1
  keep[keep.length - 1] = 1

  for (let i = 1; i < xs.length; i++) {
    const x = xs[i]
    const y = ys[i]

    if (sqDist(x, y, prevX, prevY) > epsilonSq) {
      keep[i] = 1
      prevX = x
      prevY = y
      keptLength++
    }
  }

  const simplifiedXs = new Float64Array(keptLength)
  const simplifiedYs = new Float64Array(keptLength)

  let i = 0

  for (let j = 0; j < keep.length; j++) {
    if (keep[j] === 1) {
      simplifiedXs[i] = xs[j]
      simplifiedYs[i] = ys[j]
      i++
    }
  }

  return [simplifiedXs, simplifiedYs]
}

// Shortest distance from (x2, y2) to the line segment between (x0, y0) and (x1, y1)
//
// Adapted from the [Simplify.js](https://mourner.github.io/simplify-js/) library.
const sqSegmentDist = (
  x0: number,
  y0: number,
  x1: number,
  y1: number,
  x2: number,
  y2: number
) => {
  let x = x0
  let y = y0
  let dx = x1 - x0
  let dy = y1 - y0

  if (dx !== 0 || dy !== 0) {
    var t = ((x2 - x) * dx + (y2 - y) * dy) / (dx * dx + dy * dy)

    if (t > 1) {
      x = x1
      y = y1
    } else if (t > 0) {
      x += dx * t
      y += dy * t
    }
  }

  dx = x2 - x
  dy = y2 - y

  return dx * dx + dy * dy
}

const simplifyDouglasPeuckerHelper = (
  xs: ArrayLike,
  ys: ArrayLike,
  epsilonSq: number,
  i0: number,
  i1: number,
  keep: Uint8Array
) => {
  const x0 = xs[i0]
  const y0 = ys[i0]
  const x1 = xs[i1]
  const y1 = ys[i1]

  let maxIndex = 0
  let maxDist = -1

  for (let i = i0 + 1; i < i1; i++) {
    const sqDist = sqSegmentDist(x0, y0, x1, y1, xs[i], ys[i])

    if (sqDist > maxDist) {
      maxIndex = i
      maxDist = sqDist
    }
  }

  if (maxDist > epsilonSq) {
    keep[maxIndex] = 1

    if (maxIndex - i0 > 1) {
      simplifyDouglasPeuckerHelper(xs, ys, epsilonSq, i0, maxIndex, keep)
    }

    if (i1 - maxIndex > 1) {
      simplifyDouglasPeuckerHelper(xs, ys, epsilonSq, maxIndex, i1, keep)
    }
  }
}

const simplifyDouglasPeucker = (
  xs: ArrayLike,
  ys: ArrayLike,
  epsilon: number
) => {
  const keep = new Uint8Array(xs.length)
  const sqEpsilon = epsilon * epsilon

  keep[0] = 1
  keep[keep.length - 1] = 1

  simplifyDouglasPeuckerHelper(xs, ys, sqEpsilon, 0, keep.length - 1, keep)

  let resultLength = 0

  for (let j = 0; j < keep.length; j++) {
    if (keep[j] === 1) {
      resultLength++
    }
  }

  const xsResult = new Float64Array(resultLength)
  const ysResult = new Float64Array(resultLength)

  let i = 0

  for (let j = 0; j < keep.length; j++) {
    if (keep[j] === 1) {
      xsResult[i] = xs[j]
      ysResult[i] = ys[j]
      i++
    }
  }

  return [xsResult, ysResult]
}

export const simplify = (xs: ArrayLike, ys: ArrayLike, epsilon: number) => {
  const [_xs, _ys] = simplifyDist(xs, ys, epsilon)
  const [__xs, __ys] = simplifyDouglasPeucker(_xs, _ys, epsilon)

  return [__xs, __ys]
}
