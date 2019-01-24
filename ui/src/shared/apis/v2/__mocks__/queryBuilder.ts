export const findBuckets = (_: string) => ({
  promise: Promise.resolve(['b1', 'b2']),
  cancel: () => {},
})
