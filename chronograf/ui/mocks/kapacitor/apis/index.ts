export const getLogStreamByRuleID = jest.fn(() =>
  Promise.resolve({
    body: {
      getReader: () => {},
    },
  })
)
