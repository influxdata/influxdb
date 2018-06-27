export default function extentBy<T>(
  collection: T[],
  keyFn: (v: any) => number
): T[] {
  let min = Infinity
  let max = -Infinity
  let minItem
  let maxItem

  for (const item of collection) {
    const val = keyFn(item)

    if (val <= min) {
      min = val
      minItem = item
    }

    if (val >= max) {
      max = val
      maxItem = item
    }
  }

  return [minItem, maxItem]
}
