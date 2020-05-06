/*
  Equivalent to `flatten(xs.map(f))`.
*/
export const flatMap = <X, Y>(
  f: (x: X, i: number, xs: X[]) => Y[],
  xs: X[]
): Y[] => {
  const result = []
  const yss = xs.map(f)

  for (const ys of yss) {
    for (const y of ys) {
      result.push(y)
    }
  }

  return result
}
