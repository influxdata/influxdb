export const pingKapacitorVersion = jest.fn(() => Promise.resolve('2.0'))
export const getLogStreamByRuleID = jest.fn(() =>
  Promise.resolve({
    body: {
      getReader: () => {},
    },
  })
)
