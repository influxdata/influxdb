export const findBuckets = (_: string) => ({
  promise: Promise.resolve(['b1', 'b2']),
  cancel: () => {},
})

export const findKeys = (_: string) => ({
  promise: Promise.resolve(['tk1', 'tk2']),
  cancel: () => {},
})

export const findValues = (_: string) => ({
  promise: Promise.resolve(['tv1', 'tv2']),
  cancel: () => {},
})

export const extractBoxedCol = (_: string) => ({
  promise: Promise.resolve(['Talking Heads', 'This must be the place']),
  cancel: () => {},
})
